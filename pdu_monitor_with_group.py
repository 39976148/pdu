# -*- coding: utf-8 -*-
"""
PDU 监控面板（带分组开机功能） — 在原有 pdu_monitor 基础上增加：
- 每个插座可选择分组（组名 A–J，组内序号 0–9）
- 每个字母组有一个“开机”按钮，点击后按序号顺序，每隔 10 秒依次闭合该组内的插座
"""

import sys
import os
import json
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

        self._group_start_buttons: Dict[str, QPushButton] = {}

        # 别名配置文件（与脚本同目录）
        self._alias_config_path = os.path.join(
            os.path.dirname(os.path.abspath(__file__)), "pdu_aliases.json"
        )

        self._build_ui()
        self._load_aliases()
        self._poll_timer = QTimer(self)
        self._poll_timer.timeout.connect(self._refresh_all)
        self._poll_timer.start(3000)
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

        group = QGroupBox("插座电流 / 功率（可分组开机）")
        group_layout = QVBoxLayout(group)
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

        btn_layout = QHBoxLayout()
        btn_refresh = QPushButton("立即刷新")
        btn_refresh.clicked.connect(self._refresh_all)
        btn_layout.addWidget(btn_refresh)
        btn_layout.addStretch()
        layout.addLayout(btn_layout)

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
        """任一分组下拉变化时调用。"""
        self._refresh_group_buttons_enabled()

    # ---- 别名配置文件：启动时加载，编辑完成后保存 ----
    def _load_aliases(self):
        """从 pdu_aliases.json 恢复插座别名与组别名。"""
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
        group = data.get("group_alias") or {}
        for ch in "ABCDEFGHIJ":
            if ch in group and isinstance(group[ch], str):
                self._group_alias_edits[ch].setText(group[ch])

    def _save_aliases(self):
        """将插座别名与组别名写入 pdu_aliases.json。"""
        outlet = {}
        for row in range(len(self._cell_outlet_alias)):
            for col in range(len(self._cell_outlet_alias[row])):
                text = self._cell_outlet_alias[row][col].text().strip()
                if text:
                    outlet[f"{row}_{col}"] = text
        group = {}
        for ch in "ABCDEFGHIJ":
            text = self._group_alias_edits[ch].text().strip()
            if text:
                group[ch] = text
        try:
            with open(self._alias_config_path, "w", encoding="utf-8") as f:
                json.dump({"outlet_alias": outlet, "group_alias": group}, f, ensure_ascii=False, indent=2)
        except Exception:
            pass

    def closeEvent(self, event):
        """关闭窗口时保存别名。"""
        self._save_aliases()
        super().closeEvent(event)

    # ---- SNMP 拉取与刷新，与 pdu_monitor 基本一致 ----
    def _apply_row_data(self, row: int, data: OutletRowData, total_str: str):
        is_online = row in PDU_ONLINE_ROWS
        ip = PDU_IPS[row]
        status = "在线" if is_online else "未在线"
        pdu_label = f"Pdu{row + 1} {status}\n{ip}"
        self._table.setItem(row, 0, QTableWidgetItem(f"{pdu_label}\n功率 {total_str}"))
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
        self._status.showMessage("实时刷新中（每 3 秒）")

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

    # ---- 组开机逻辑：按组内序号顺序，每 10 秒一批依次闭合（同一序号的插座同时闭合）----
    def _on_group_start(self, group_letter: str):
        groups = self._collect_groups()
        if group_letter not in groups or not groups[group_letter]:
            self._status.showMessage(f"{group_letter} 组当前没有配置插座")
            return
        # 将同一序号的插座视为一批，同批同时闭合：如 A0（1 个）、A1（2 个）、A2（2 个）
        # 效果：先 A0，10 秒后两个 A1 同时闭合，再 10 秒后两个 A2 同时闭合
        items = groups[group_letter]
        # order -> [(row, col), ...]
        order_buckets: Dict[int, List[Tuple[int, int]]] = {}
        for order, row, col in items:
            order_buckets.setdefault(order, []).append((row, col))
        sorted_orders = sorted(order_buckets.keys())

        interval_ms = 10_000
        for step_idx, order in enumerate(sorted_orders):
            delay = step_idx * interval_ms
            for row, col in order_buckets[order]:
                QTimer.singleShot(
                    delay,
                    lambda r=row, c=col: self._group_on_outlet_control(r, c, True),
                )

        # 状态栏提示：按批次显示
        names_parts = []
        for order in sorted_orders:
            members = order_buckets[order]
            member_str = ", ".join(f"Pdu{r + 1}-插座{c + 1}" for r, c in members)
            names_parts.append(f"#{order}: {member_str}")
        names = " | ".join(names_parts)
        self._status.showMessage(
            f"已启动 {group_letter} 组开机顺序（间隔 10 秒）：{names}"
        )


def main():
    app = QApplication(sys.argv)
    app.setStyle("Fusion")
    win = PduMonitorWithGroupWindow()
    win.show()
    sys.exit(app.exec())


if __name__ == "__main__":
    main()

