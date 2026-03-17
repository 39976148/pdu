# -*- coding: utf-8 -*-
"""
PDU 监控面板（带分组开机功能） — 在原有 pdu_monitor 基础上增加：
- 每个插座可选择分组（组名 A–J，组内序号 0–9）
- 每个字母组有一个“开机”按钮，点击后按序号顺序，每隔 10 秒依次闭合该组内的插座
"""

import sys
import os
import json
import csv
from datetime import datetime
from typing import List, Tuple, Optional, Dict

from PySide6.QtWidgets import (
    QApplication,
    QMainWindow,
    QWidget,
    QVBoxLayout,
    QHBoxLayout,
    QTableWidget,
    QTableWidgetItem,
    QHeaderView,
    QLabel,
    QPushButton,
    QGroupBox,
    QStatusBar,
    QFrame,
    QComboBox,
    QLineEdit,
    QMessageBox,
)
from PySide6.QtCore import Qt, QTimer, QThread, Signal

import asyncio

# 直接复用 pdu_monitor 中的 SNMP 配置和逻辑
PDU_IPS: List[str] = [
    "192.168.1.161",
    "192.168.1.162",
    "192.168.1.163",
    "192.168.1.164",
    "192.168.1.165",
    "192.168.1.166",
]
PDU_ONLINE_ROWS = (2,)
OUTLETS_PER_PDU = 6
SNMP_PORT = 161
SNMP_COMMUNITY = "public"
SNMP_WRITE_COMMUNITY = "private"
OID_OUTLET_CURRENT_BASE = "1.3.6.1.4.1.23280.8.1.4"
OID_OUTLET_POWER_BASE = "1.3.6.1.4.1.23280.8.1.5"
OID_OUTLET_STATE_BASE = "1.3.6.1.4.1.23280.8.1.2"
OID_OUTLET_CONTROL_BASE = "1.3.6.1.4.1.23280.9.1.2"
CURRENT_DIVISOR = 100

try:
    from pysnmp.hlapi.v3arch.asyncio import (
        SnmpEngine,
        get_cmd,
        set_cmd,
        CommunityData,
        UdpTransportTarget,
        ContextData,
    )
    from pysnmp.smi.rfc1902 import ObjectType, ObjectIdentity
    from pysnmp.proto.rfc1902 import Integer32
    HAS_PYSNMP = True
except ImportError:
    HAS_PYSNMP = False
    set_cmd = None
    Integer32 = None


async def _snmp_get_async(host: str, oid: str, community: str, port: int) -> Optional[float]:
    engine = SnmpEngine()
    try:
        result = await get_cmd(
            engine,
            CommunityData(community, mpModel=0),
            await UdpTransportTarget.create((host, port), timeout=3, retries=5),
            ContextData(),
            ObjectType(ObjectIdentity(oid)),
        )
        err_ind, err_status, _, var_binds = result
        if err_ind or err_status or not var_binds:
            return None
        val = var_binds[0][1]
        return float(val) if val is not None else None
    except Exception:
        return None
    finally:
        engine.close_dispatcher()


def snmp_get(host: str, oid: str, community: str = SNMP_COMMUNITY, port: int = SNMP_PORT) -> Optional[float]:
    if not HAS_PYSNMP:
        return None
    return asyncio.run(_snmp_get_async(host, oid, community, port))


async def _snmp_set_async(host: str, oid: str, value: int, community: str, port: int) -> bool:
    if not HAS_PYSNMP or set_cmd is None or Integer32 is None:
        return False
    engine = SnmpEngine()
    try:
        result = await set_cmd(
            engine,
            CommunityData(community, mpModel=0),
            await UdpTransportTarget.create((host, port), timeout=3, retries=2),
            ContextData(),
            ObjectType(ObjectIdentity(oid), Integer32(value)),
        )
        err_ind, err_status, _, _ = result
        return not (err_ind or err_status)
    except Exception:
        return False
    finally:
        engine.close_dispatcher()


def snmp_set(host: str, oid: str, value: int, community: str = SNMP_COMMUNITY, port: int = SNMP_PORT) -> bool:
    if not OID_OUTLET_CONTROL_BASE or not HAS_PYSNMP:
        return False
    return asyncio.run(_snmp_set_async(host, oid, value, community, port))


def read_outlet_current(host: str, outlet_index: int) -> Optional[float]:
    oid = f"{OID_OUTLET_CURRENT_BASE}.{outlet_index}"
    raw = snmp_get(host, oid)
    if raw is None:
        return None
    return round(raw / CURRENT_DIVISOR, 3)


def read_outlet_power(host: str, outlet_index: int) -> Optional[float]:
    oid = f"{OID_OUTLET_POWER_BASE}.{outlet_index}"
    raw = snmp_get(host, oid)
    if raw is None:
        return None
    return round(raw, 1)


OutletRowData = List[Tuple[Optional[float], Optional[float], Optional[bool]]]


async def fetch_pdu_row_data_async(ip: str) -> OutletRowData:
    result: OutletRowData = []
    for i in range(1, OUTLETS_PER_PDU + 1):
        oid_c = f"{OID_OUTLET_CURRENT_BASE}.{i}"
        oid_p = f"{OID_OUTLET_POWER_BASE}.{i}"
        oid_s = f"{OID_OUTLET_STATE_BASE}.{i}"
        raw_c = await _snmp_get_async(ip, oid_c, SNMP_COMMUNITY, SNMP_PORT)
        raw_p = await _snmp_get_async(ip, oid_p, SNMP_COMMUNITY, SNMP_PORT)
        raw_s = await _snmp_get_async(ip, oid_s, SNMP_COMMUNITY, SNMP_PORT)
        cur = round(raw_c / CURRENT_DIVISOR, 3) if raw_c is not None else None
        pwr = round(raw_p, 1) if raw_p is not None else None
        state = (int(raw_s) == 2) if raw_s is not None else None
        result.append((cur, pwr, state))
    return result


async def fetch_all_online_async() -> Dict[int, OutletRowData]:
    out: Dict[int, OutletRowData] = {}
    for row in PDU_ONLINE_ROWS:
        ip = PDU_IPS[row]
        try:
            out[row] = await fetch_pdu_row_data_async(ip)
        except Exception:
            out[row] = [(None, None, None)] * OUTLETS_PER_PDU
    return out


def set_outlet_on_off(host: str, outlet_index: int, on: bool) -> bool:
    if not OID_OUTLET_CONTROL_BASE:
        return False
    oid = f"{OID_OUTLET_CONTROL_BASE}.{outlet_index}"
    cmd = 1 if on else 2
    return snmp_set(host, oid, cmd, community=SNMP_WRITE_COMMUNITY)


class OutletSetWorker(QThread):
    set_done = Signal(bool, int, int, bool)

    def __init__(self, ip: str, outlet_index: int, on: bool, row: int, col: int):
        super().__init__()
        self._ip = ip
        self._outlet_index = outlet_index
        self._on = on
        self._row = row
        self._col = col

    def run(self):
        ok = set_outlet_on_off(self._ip, self._outlet_index, self._on)
        self.set_done.emit(ok, self._row, self._col, self._on)


class AllOffWorker(QThread):
    """后台依次断开所有在线 PDU 的全部插座。"""
    finished_all = Signal()

    def __init__(self, rows: List[int]):
        super().__init__()
        self._rows = rows  # PDU_ONLINE_ROWS

    def run(self):
        for row in self._rows:
            ip = PDU_IPS[row]
            for outlet_index in range(1, OUTLETS_PER_PDU + 1):
                set_outlet_on_off(ip, outlet_index, False)
        self.finished_all.emit()


class SnmpFetchWorker(QThread):
    data_ready = Signal()

    def __init__(self):
        super().__init__()
        self.result: Dict[int, OutletRowData] = {}

    def run(self):
        if not HAS_PYSNMP or not PDU_ONLINE_ROWS:
            self.result = {}
            self.data_ready.emit()
            return
        try:
            self.result = asyncio.run(fetch_all_online_async())
        except Exception:
            self.result = {}
        self.data_ready.emit()


class PduMonitorWithGroupWindow(QMainWindow):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("PDU 监控（带分组开机）— 6 台 × 6 插座")
        self.setMinimumSize(1300, 650)
        self._worker_busy = False
        self._worker = None
        self._set_worker = None
        self._all_off_worker = None  # 全部断开后台线程

        self._group_start_buttons: Dict[str, QPushButton] = {}

        # 别名配置文件（与脚本同目录）
        self._alias_config_path = os.path.join(
            os.path.dirname(os.path.abspath(__file__)), "pdu_aliases.json"
        )
        self._pdu_data_dir = os.path.join(
            os.path.dirname(os.path.abspath(__file__)), "pdu_data"
        )
        # 本次运行使用的 CSV / 别名 JSON（每次启动一个带时间戳的文件，本运行内追加）
        _ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        self._run_csv_path = os.path.join(
            self._pdu_data_dir, f"pdu_data_{_ts}.csv",
        )
        self._run_alias_path = os.path.join(
            self._pdu_data_dir, f"pdu_alias_{_ts}.json",
        )
        self._last_data_by_row: Dict[int, OutletRowData] = {}  # 最近一次每行数据，供保存 CSV

        self._build_ui()
        self._load_aliases()
        self._poll_timer = QTimer(self)
        self._poll_timer.timeout.connect(self._refresh_all)
        self._poll_timer.start(1000)  # 1 秒刷新间隔，试验性能；与自动保存 CSV 间隔一致
        self._refresh_all()

    def _build_ui(self):
        central = QWidget()
        self.setCentralWidget(central)
        layout = QVBoxLayout(central)

        info = QLabel(
            "PDU 监控（带分组） | 型号 v3L | SNMP V1 | "
            "IP: 161–166，当前连接: 192.168.1.163（Pdu3）"
        )
        info.setStyleSheet("color: #666; padding: 4px;")
        layout.addWidget(info)

        group = QGroupBox("")
        group_layout = QVBoxLayout(group)

        # 同一行：立即刷新、保存数据（固定大小），全部断开占满剩余
        top_btn_row = QHBoxLayout()
        btn_refresh = QPushButton("立即刷新")
        btn_refresh.setFixedSize(100, 56)
        btn_refresh.clicked.connect(self._refresh_all)
        top_btn_row.addWidget(btn_refresh)

        btn_save_data = QPushButton("保存数据")
        btn_save_data.setFixedSize(100, 56)
        btn_save_data.clicked.connect(self._save_data_to_files)
        top_btn_row.addWidget(btn_save_data)

        btn_all_off = QPushButton("全部断开")
        btn_all_off.setMinimumHeight(56)
        btn_all_off.setStyleSheet(
            "font-size: 18px; font-weight: bold; background-color: #c44; color: white;"
            "border: 2px solid #800; border-radius: 6px;"
        )
        btn_all_off.setCursor(Qt.CursorShape.PointingHandCursor)
        btn_all_off.clicked.connect(self._on_all_off_clicked)
        top_btn_row.addWidget(btn_all_off, 1)  # stretch=1 占满剩余
        group_layout.addLayout(top_btn_row)

        self._table = QTableWidget(6, 1 + OUTLETS_PER_PDU)
        self._table.setHorizontalHeaderLabels(
            ["PDU / 总功率"] + [f"插座{i}" for i in range(1, OUTLETS_PER_PDU + 1)]
        )
        self._table.horizontalHeader().setSectionResizeMode(QHeaderView.ResizeMode.Fixed)
        self._table.verticalHeader().setVisible(True)
        self._table.setAlternatingRowColors(True)
        self._table.setStyleSheet(
            """
            QTableWidget { font-size: 12px; gridline-color: #ddd; }
            QHeaderView::section { background: #e8e8e8; padding: 6px; font-weight: bold; }
            """
        )

        first_col_width = 190
        outlet_col_width = 170
        row_height = 88

        self._table.setColumnWidth(0, first_col_width)
        for col in range(OUTLETS_PER_PDU):
            self._table.setColumnWidth(col + 1, outlet_col_width)

        for row in range(6):
            self._table.setVerticalHeaderItem(row, QTableWidgetItem(f"Pdu{row + 1}"))
            self._table.setRowHeight(row, row_height)

        STATUS_INDICATOR_W = 30
        STATUS_INDICATOR_H = 18

        self._cell_status_labels: List[List[QFrame]] = []
        self._cell_btn_on: List[List[QPushButton]] = []
        self._cell_btn_off: List[List[QPushButton]] = []
        self._cell_current_labels: List[List[QLabel]] = []
        self._cell_power_labels: List[List[QLabel]] = []
        self._outlet_states: List[List[bool]] = []
        # 插座别名输入（每个插座一行文本）
        self._cell_outlet_alias: List[List[QLineEdit]] = []
        # 分组下拉：组名(A–J)与组内序号(0–9)
        self._cell_group_letter: List[List[QComboBox]] = []
        self._cell_group_index: List[List[QComboBox]] = []

        for row in range(6):
            self._cell_status_labels.append([])
            self._cell_current_labels.append([])
            self._cell_power_labels.append([])
            self._cell_btn_on.append([])
            self._cell_btn_off.append([])
            self._cell_outlet_alias.append([])
            self._cell_group_letter.append([])
            self._cell_group_index.append([])
            self._outlet_states.append([False] * OUTLETS_PER_PDU)
            for col in range(OUTLETS_PER_PDU):
                frame = QWidget()
                frame.setMinimumSize(outlet_col_width - 8, row_height - 6)
                lay = QVBoxLayout(frame)
                lay.setContentsMargins(2, 1, 2, 1)
                lay.setSpacing(0)

                # 最上方一行：插座别名（可输入文本，保存到配置文件）
                alias_edit = QLineEdit()
                alias_edit.setPlaceholderText("别名")
                alias_edit.setMaxLength(32)
                alias_edit.setStyleSheet("font-size: 11px;")
                alias_edit.editingFinished.connect(self._save_aliases)
                self._cell_outlet_alias[row].append(alias_edit)
                lay.addWidget(alias_edit)

                # 状态长方形 + 开/关按钮 + 分组选择（字母+序号）
                top = QHBoxLayout()
                status_indicator = QFrame()
                status_indicator.setFixedSize(STATUS_INDICATOR_W, STATUS_INDICATOR_H)
                status_indicator.setStyleSheet(
                    "background-color: #c00; border: 1px solid #888; border-radius: 2px;"
                )
                self._cell_status_labels[row].append(status_indicator)
                top.addWidget(status_indicator)

                btn_on = QPushButton("开")
                btn_on.setFixedWidth(32)
                font_on = btn_on.font()
                font_on.setBold(True)
                btn_on.setFont(font_on)
                btn_on.clicked.connect(
                    lambda _=False, r=row, c=col: self._on_outlet_control(r, c, True)
                )
                self._cell_btn_on[row].append(btn_on)
                top.addWidget(btn_on)

                btn_off = QPushButton("关")
                btn_off.setFixedWidth(32)
                font_off = btn_off.font()
                font_off.setBold(True)
                btn_off.setFont(font_off)
                btn_off.clicked.connect(
                    lambda _=False, r=row, c=col: self._on_outlet_control(r, c, False)
                )
                self._cell_btn_off[row].append(btn_off)
                top.addWidget(btn_off)

                # 分组下拉：字母组 + 组内序号
                cmb_group = QComboBox()
                cmb_group.setFixedWidth(60)
                cmb_group.addItem("")  # 空表示不参与分组
                for ch in "ABCDEFGHIJ":
                    cmb_group.addItem(ch)

                cmb_index = QComboBox()
                cmb_index.setFixedWidth(55)
                cmb_index.addItem("")
                for i in range(10):
                    cmb_index.addItem(str(i))

                # 任一分组下拉变化，刷新“组开机”按钮可用状态
                cmb_group.currentIndexChanged.connect(self._on_group_config_changed)
                cmb_index.currentIndexChanged.connect(self._on_group_config_changed)

                self._cell_group_letter[row].append(cmb_group)
                self._cell_group_index[row].append(cmb_index)
                top.addWidget(cmb_group)
                top.addWidget(cmb_index)

                top.addStretch()
                lay.addLayout(top)

                # 底部：电流 / 功率 标签
                cur_lbl = QLabel("电流 -A")
                pwr_lbl = QLabel("功率 -W")
                cur_lbl.setAlignment(Qt.AlignmentFlag.AlignLeft | Qt.AlignmentFlag.AlignVCenter)
                pwr_lbl.setAlignment(Qt.AlignmentFlag.AlignLeft | Qt.AlignmentFlag.AlignVCenter)
                self._cell_current_labels[row].append(cur_lbl)
                self._cell_power_labels[row].append(pwr_lbl)
                lay.addWidget(cur_lbl)
                lay.addWidget(pwr_lbl)

                self._table.setCellWidget(row, col + 1, frame)

        group_layout.addWidget(self._table)

        # 组开机区域：一行 10 个 frame，每个 frame 内上为“X组开机”按钮、下为组别名输入，上下对齐
        self._group_alias_edits: Dict[str, QLineEdit] = {}
        group_row = QHBoxLayout()
        group_row.addWidget(QLabel("分组开机 / 组别名："))
        for ch in "ABCDEFGHIJ":
            cell = QFrame()
            cell.setFrameStyle(QFrame.Shape.StyledPanel | QFrame.Shadow.Plain)
            cell_layout = QVBoxLayout(cell)
            cell_layout.setContentsMargins(4, 2, 4, 2)
            cell_layout.setSpacing(2)
            btn = QPushButton(f"{ch}组开机")
            btn.setEnabled(False)
            btn.clicked.connect(lambda _=False, g=ch: self._on_group_start(g))
            self._group_start_buttons[ch] = btn
            cell_layout.addWidget(btn)
            le = QLineEdit()
            le.setPlaceholderText(f"{ch}组别名")
            le.setMaxLength(24)
            le.setFixedWidth(72)
            self._group_alias_edits[ch] = le
            le.editingFinished.connect(self._save_aliases)
            cell_layout.addWidget(le)
            group_row.addWidget(cell)
        group_row.addStretch()
        group_layout.addLayout(group_row)

        layout.addWidget(group)

        self._status = QStatusBar()
        self.setStatusBar(self._status)
        if not HAS_PYSNMP:
            self._status.showMessage("未安装 pysnmp，当前为模拟数据。请执行: pip install pysnmp")

    # ---- 分组相关工具方法 ----
    def _collect_groups(self) -> Dict[str, List[Tuple[int, int, int]]]:
        """
        收集当前分组配置。
        返回: {group_letter: [(order, row, col), ...]}
        """
        groups: Dict[str, List[Tuple[int, int, int]]] = {}
        for row in range(6):
            for col in range(OUTLETS_PER_PDU):
                if row >= len(self._cell_group_letter) or col >= len(self._cell_group_letter[row]):
                    continue
                cmb_g = self._cell_group_letter[row][col]
                cmb_i = self._cell_group_index[row][col]
                g = cmb_g.currentText().strip()
                idx_text = cmb_i.currentText().strip()
                if not g or not idx_text:
                    continue
                try:
                    order = int(idx_text)
                except ValueError:
                    continue
                groups.setdefault(g, []).append((order, row, col))
        return groups

    def _refresh_group_buttons_enabled(self):
        """根据当前分组配置，启用或禁用各组的“开机”按钮。"""
        groups = self._collect_groups()
        for ch, btn in self._group_start_buttons.items():
            btn.setEnabled(ch in groups and len(groups[ch]) > 0)

    def _on_group_config_changed(self, *_):
        """任一分组下拉变化时：刷新组开机按钮状态，并保存别名/分组到配置文件。"""
        self._refresh_group_buttons_enabled()
        self._save_aliases()

    # ---- 别名配置文件：启动时加载，编辑完成后保存 ----
    def _load_aliases(self):
        """从 pdu_aliases.json 恢复插座别名、分组（A0/A1 等）与组别名。"""
        if not os.path.isfile(self._alias_config_path):
            return
        try:
            with open(self._alias_config_path, "r", encoding="utf-8") as f:
                data = json.load(f)
        except Exception:
            return
        outlet = data.get("outlet_alias") or {}
        for row in range(6):
            for col in range(OUTLETS_PER_PDU):
                if row >= len(self._cell_outlet_alias) or col >= len(self._cell_outlet_alias[row]):
                    continue
                key = f"{row}_{col}"
                if key in outlet and isinstance(outlet[key], str):
                    self._cell_outlet_alias[row][col].setText(outlet[key])
        # 恢复分组：每个插座的组字母与组内序号，避免重启后 A 组开机不可用
        outlet_group = data.get("outlet_group") or {}
        for row in range(6):
            for col in range(OUTLETS_PER_PDU):
                if row >= len(self._cell_group_letter) or col >= len(self._cell_group_letter[row]):
                    continue
                key = f"{row}_{col}"
                if key not in outlet_group or not isinstance(outlet_group[key], dict):
                    continue
                let = outlet_group[key].get("letter", "")
                idx = outlet_group[key].get("index", "")
                cmb_g = self._cell_group_letter[row][col]
                cmb_i = self._cell_group_index[row][col]
                cmb_g.blockSignals(True)
                cmb_i.blockSignals(True)
                if cmb_g.findText(let) >= 0:
                    cmb_g.setCurrentText(let)
                if cmb_i.findText(idx) >= 0:
                    cmb_i.setCurrentText(idx)
                cmb_g.blockSignals(False)
                cmb_i.blockSignals(False)
        group = data.get("group_alias") or {}
        for ch in "ABCDEFGHIJ":
            if ch in group and isinstance(group[ch], str):
                self._group_alias_edits[ch].setText(group[ch])
        self._refresh_group_buttons_enabled()

    def _save_aliases(self):
        """每次别名或分组发生修改时：
        1）覆盖写入 pdu_aliases.json，仅保留最后一次结果，供下次启动恢复；
        2）向本次运行的 pdu_data/pdu_alias_yyyymmdd_hhmmss.json 追加一条记录（JSON 数组，不新建文件）。
        """
        outlet = {}
        for row in range(len(self._cell_outlet_alias)):
            for col in range(len(self._cell_outlet_alias[row])):
                text = self._cell_outlet_alias[row][col].text().strip()
                if text:
                    outlet[f"{row}_{col}"] = text
        outlet_group = {}
        for row in range(len(self._cell_group_letter)):
            for col in range(len(self._cell_group_letter[row])):
                key = f"{row}_{col}"
                g = self._cell_group_letter[row][col].currentText().strip()
                idx = self._cell_group_index[row][col].currentText().strip()
                if g or idx:
                    outlet_group[key] = {"letter": g, "index": idx}
        group = {}
        for ch in "ABCDEFGHIJ":
            text = self._group_alias_edits[ch].text().strip()
            if text:
                group[ch] = text
        now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        payload = {
            "outlet_alias": outlet,
            "outlet_group": outlet_group,
            "group_alias": group,
            "updated_at": now_str,
        }
        try:
            with open(self._alias_config_path, "w", encoding="utf-8") as f:
                json.dump(payload, f, ensure_ascii=False, indent=2)
        except Exception:
            pass
        # 向本次运行的 pdu_alias_yyyymmdd_hhmmss.json 追加一条（文件为 JSON 数组）
        try:
            os.makedirs(self._pdu_data_dir, exist_ok=True)
            records: List[dict] = []
            if os.path.isfile(self._run_alias_path) and os.path.getsize(self._run_alias_path) > 0:
                with open(self._run_alias_path, "r", encoding="utf-8") as f:
                    records = json.load(f)
                if not isinstance(records, list):
                    records = [records]
            records.append(payload)
            with open(self._run_alias_path, "w", encoding="utf-8") as f:
                json.dump(records, f, ensure_ascii=False, indent=2)
        except Exception:
            pass

    def _get_csv_headers(self) -> List[str]:
        """返回 CSV 表头。"""
        headers = ["date_time"]
        for pdu_num in range(1, 7):
            headers.append(f"pdu{pdu_num}_total_power")
            for out in range(1, OUTLETS_PER_PDU + 1):
                headers.append(f"pdu{pdu_num}_outlet{out}_current")
                headers.append(f"pdu{pdu_num}_outlet{out}_power")
        return headers

    def _append_csv_row(self) -> Optional[str]:
        """向本次运行的 CSV 文件追加一行（文件不存在或为空时先写表头）。返回路径；失败返回 None。"""
        os.makedirs(self._pdu_data_dir, exist_ok=True)
        csv_path = self._run_csv_path
        headers = self._get_csv_headers()
        row_values = [datetime.now().strftime("%Y-%m-%d %H:%M:%S")]
        for row in range(6):
            data = self._last_data_by_row.get(row, [(None, None, None)] * OUTLETS_PER_PDU)
            total = sum((p or 0) for _, p, _ in data)
            row_values.append(str(int(total)) if total else "")
            for col in range(OUTLETS_PER_PDU):
                cur, pwr = (data[col][0], data[col][1]) if col < len(data) else (None, None)
                row_values.append(f"{cur:.3f}" if cur is not None else "")
                row_values.append(f"{pwr:.1f}" if pwr is not None else "")
        try:
            write_header = not os.path.isfile(csv_path) or os.path.getsize(csv_path) == 0
            with open(csv_path, "a", newline="", encoding="utf-8-sig") as f:
                w = csv.writer(f)
                if write_header:
                    w.writerow(headers)
                w.writerow(row_values)
            return csv_path
        except Exception:
            return None

    def _save_csv_auto(self):
        """按刷新间隔自动向本次运行的 CSV 文件追加一行。"""
        path = self._append_csv_row()
        if path:
            self._status.showMessage(f"已自动保存 CSV（每 1 秒）: {os.path.basename(path)}")

    def _save_data_to_files(self):
        """向本次运行的 CSV 追加一行，并写入带时间戳的别名 JSON 到 pdu_data 目录。"""
        csv_path = self._append_csv_row()
        if csv_path is None:
            self._status.showMessage("保存 CSV 失败")
            return

        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        alias_path = os.path.join(self._pdu_data_dir, f"pdu_alias_{ts}.json")
        outlet_alias = {}
        outlet_group = {}
        for row in range(len(self._cell_outlet_alias)):
            for col in range(len(self._cell_outlet_alias[row])):
                key = f"{row}_{col}"
                t = self._cell_outlet_alias[row][col].text().strip()
                if t:
                    outlet_alias[key] = t
                g = self._cell_group_letter[row][col].currentText().strip()
                idx = self._cell_group_index[row][col].currentText().strip()
                if g or idx:
                    outlet_group[key] = {"letter": g, "index": idx}
        group_alias = {}
        for ch in "ABCDEFGHIJ":
            t = self._group_alias_edits[ch].text().strip()
            if t:
                group_alias[ch] = t
        alias_payload = {
            "outlet_alias": outlet_alias,
            "outlet_group": outlet_group,
            "group_alias": group_alias,
            "saved_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        }
        try:
            with open(alias_path, "w", encoding="utf-8") as f:
                json.dump(alias_payload, f, ensure_ascii=False, indent=2)
        except Exception as e:
            self._status.showMessage(f"保存别名 JSON 失败: {e}")
            return

        self._status.showMessage(f"已保存: {csv_path} 与 {alias_path}")
        QMessageBox.information(
            self, "保存完成",
            f"已保存到 pdu_data 目录：\n{os.path.basename(csv_path)}\n{os.path.basename(alias_path)}",
        )

    # ---- SNMP 拉取与刷新，与 pdu_monitor 基本一致 ----
    def _apply_row_data(self, row: int, data: OutletRowData, total_str: str):
        is_online = row in PDU_ONLINE_ROWS
        ip = PDU_IPS[row]
        status = "在线" if is_online else "未在线"
        pdu_label = f"Pdu{row + 1} {status}\n{ip}"
        self._table.setItem(row, 0, QTableWidgetItem(f"{pdu_label}\n功率 {total_str}"))
        self._last_data_by_row[row] = list(data)  # 供保存 CSV 使用
        for col, item in enumerate(data):
            if col >= len(self._cell_current_labels[row]):
                break
            cur, pwr, state = item if len(item) >= 3 else (item[0], item[1], None)
            cur_text = f"{cur:.3f}A" if cur is not None else "-A"
            pwr_text = f"{pwr:.1f}W" if pwr is not None else "-W"
            cur_full = f"电流 {cur_text}"
            pwr_full = f"功率 {pwr_text}"
            if self._cell_current_labels[row][col].text() != cur_full:
                self._cell_current_labels[row][col].setText(cur_full)
            if self._cell_power_labels[row][col].text() != pwr_full:
                self._cell_power_labels[row][col].setText(pwr_full)
            status_indicator = self._cell_status_labels[row][col]
            want = "#888" if not is_online else ("#0a0" if state is True else "#c00")
            status_indicator.setStyleSheet(
                f"background-color: {want}; border: 1px solid #666; border-radius: 2px;"
            )
            if state is not None and row < len(self._outlet_states) and col < len(self._outlet_states[row]):
                self._outlet_states[row][col] = state
            if 0 <= row < len(self._cell_btn_on) and 0 <= col < len(self._cell_btn_on[row]):
                btn_on = self._cell_btn_on[row][col]
                btn_off = self._cell_btn_off[row][col]
                if not is_online or state is None:
                    btn_on.setEnabled(False)
                    btn_off.setEnabled(False)
                else:
                    btn_on.setEnabled(not state)
                    btn_off.setEnabled(state)

        # 每次刷完一行后，整体刷新一下组按钮可用状态
        self._refresh_group_buttons_enabled()

    def _on_worker_finished(self):
        self._worker_busy = False
        if self._worker:
            self._worker.deleteLater()
            self._worker = None

    def _on_pdu_data(self):
        data_by_row = self._worker.result if self._worker else {}
        for row, data in data_by_row.items():
            if row >= 6 or len(data) != OUTLETS_PER_PDU:
                continue
            total_power = sum((p or 0) for _, p, _ in data)
            total_str = f"{int(total_power)}W" if total_power else "-"
            self._apply_row_data(row, data, total_str)
        self._save_csv_auto()  # 与刷新间隔一致，每次刷新完成即自动保存 CSV
        self._status.showMessage("实时刷新中（每 1 秒），已自动保存 CSV")

    def _refresh_all(self):
        for row, ip in enumerate(PDU_IPS):
            if row not in PDU_ONLINE_ROWS:
                data = [(None, None, None)] * OUTLETS_PER_PDU
                self._apply_row_data(row, data, "-")

        if not self._worker_busy and HAS_PYSNMP and PDU_ONLINE_ROWS:
            self._worker_busy = True
            self._worker = SnmpFetchWorker()
            self._worker.data_ready.connect(self._on_pdu_data)
            self._worker.finished.connect(self._on_worker_finished)
            self._worker.start()

    def _on_set_done(self, success: bool, row: int, col: int, on: bool):
        if self._set_worker:
            self._set_worker.deleteLater()
            self._set_worker = None
        outlet_index = col + 1
        if success:
            self._outlet_states[row][col] = on
            self._status.showMessage(f"Pdu{row + 1} 插座{outlet_index} 已{'开' if on else '关'}")
            try:
                btn_on = self._cell_btn_on[row][col]
                btn_off = self._cell_btn_off[row][col]
                btn_on.setEnabled(not on)
                btn_off.setEnabled(on)
            except Exception:
                pass
        else:
            self._status.showMessage(
                f"Pdu{row + 1} 插座{outlet_index} 控制失败（请检查 OID/SNMP 写权限）"
            )

    def _on_outlet_control(self, row: int, col: int, on: bool):
        """单个插座通过按钮操作时的控制（仍保持串行，避免误触频繁操作）。"""
        ip = PDU_IPS[row]
        if row not in PDU_ONLINE_ROWS:
            self._status.showMessage(f"Pdu{row + 1} 未在线，无法控制")
            return
        if self._set_worker and self._set_worker.isRunning():
            self._status.showMessage("上一指令执行中，请稍候")
            return
        outlet_index = col + 1
        self._set_worker = OutletSetWorker(ip, outlet_index, on, row, col)
        self._set_worker.set_done.connect(self._on_set_done)
        self._set_worker.start()
        self._status.showMessage(f"正在发送 插座{outlet_index} {'开' if on else '关'}…")

    def _on_all_off_clicked(self):
        """全部断开：先确认，再断开所有在线 PDU 的全部插座。"""
        if not PDU_ONLINE_ROWS:
            self._status.showMessage("当前无在线 PDU")
            return
        if self._all_off_worker and self._all_off_worker.isRunning():
            self._status.showMessage("正在执行全部断开，请稍候")
            return
        reply = QMessageBox.question(
            self,
            "确认全部断开",
            "确定要断开所有在线 PDU 的全部插座吗？",
            QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No,
            QMessageBox.StandardButton.No,
        )
        if reply != QMessageBox.StandardButton.Yes:
            return
        self._all_off_worker = AllOffWorker(list(PDU_ONLINE_ROWS))
        self._all_off_worker.finished_all.connect(self._on_all_off_done)
        self._all_off_worker.start()
        self._status.showMessage("正在断开全部插座…")

    def _on_all_off_done(self):
        """全部断开后台完成：刷新界面、释放 worker 并弹出完成提示。"""
        if self._all_off_worker:
            self._all_off_worker.deleteLater()
            self._all_off_worker = None
        self._refresh_all()
        self._status.showMessage("全部插座已断开")
        QMessageBox.information(self, "全部断开完成", "全部插座已断开。")

    def _group_on_outlet_control(self, row: int, col: int, on: bool):
        """组开机使用的控制：允许同一批内多个插座并行执行，不受 _set_worker 串行限制。"""
        ip = PDU_IPS[row]
        if row not in PDU_ONLINE_ROWS:
            return
        outlet_index = col + 1

        worker = OutletSetWorker(ip, outlet_index, on, row, col)

        def _on_done(success: bool, r: int, c: int, o: bool, w: OutletSetWorker = worker):
            # 复用现有的 UI 更新逻辑，但不依赖 self._set_worker
            outlet_idx = c + 1
            if success:
                self._outlet_states[r][c] = o
                self._status.showMessage(f"[组开机] Pdu{r + 1} 插座{outlet_idx} 已{'开' if o else '关'}")
                try:
                    btn_on = self._cell_btn_on[r][c]
                    btn_off = self._cell_btn_off[r][c]
                    btn_on.setEnabled(not o)
                    btn_off.setEnabled(o)
                except Exception:
                    pass
            w.deleteLater()

        worker.set_done.connect(_on_done)
        worker.start()

    # ---- 组开机逻辑：先查状态，仅对当前断开的插座按序号顺序、每 10 秒间隔发送闭合 ----
    def _on_group_start(self, group_letter: str):
        groups = self._collect_groups()
        if group_letter not in groups or not groups[group_letter]:
            self._status.showMessage(f"{group_letter} 组当前没有配置插座")
            return
        items = groups[group_letter]
        # 只保留当前为“断开”的插座（需要闭合的），并按 order 排序
        need_close: List[Tuple[int, int, int]] = []
        for order, row, col in items:
            if row >= len(self._outlet_states) or col >= len(self._outlet_states[row]):
                continue
            if self._outlet_states[row][col] is False:
                need_close.append((order, row, col))
        need_close.sort(key=lambda x: x[0])

        if not need_close:
            self._status.showMessage(f"{group_letter} 组内插座均已闭合，无需操作")
            return

        interval_ms = 10_000
        for step_idx, (_, row, col) in enumerate(need_close):
            delay = step_idx * interval_ms
            QTimer.singleShot(
                delay,
                lambda r=row, c=col: self._group_on_outlet_control(r, c, True),
            )

        names = ", ".join(f"Pdu{r + 1}-插座{c + 1}#{order}" for order, r, c in need_close)
        self._status.showMessage(
            f"已启动 {group_letter} 组开机（仅对当前断开的插座，间隔 10 秒）：{names}"
        )


def main():
    app = QApplication(sys.argv)
    app.setStyle("Fusion")
    win = PduMonitorWithGroupWindow()
    win.show()
    sys.exit(app.exec())


if __name__ == "__main__":
    main()

