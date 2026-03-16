# -*- coding: utf-8 -*-
"""
PDU 监控面板 — 6 台 PDU（v3L），每台 6 个插座，通过 SNMP V1 读取电流/功率/插座状态。

- 型号: v3L，支持 SNMP V1、TCP、Telnet、Modbus
- IP 分配: 192.168.1.161–166（Pdu1–Pdu6）；当前已连接: 192.168.1.163 → Pdu3，Pdu1/2/4/5/6 未在线。
"""
import sys
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
)
from PySide6.QtCore import Qt, QTimer, QThread, Signal
from PySide6.QtGui import QFont

# 6 台 PDU 的 IP：161–166；192.168.1.163 分配给 Pdu3（当前唯一在线）
PDU_IPS: List[str] = [
    "192.168.1.161",  # Pdu1 未在线
    "192.168.1.162",  # Pdu2 未在线
    "192.168.1.163",  # Pdu3 已连接
    "192.168.1.164",  # Pdu4 未在线
    "192.168.1.165",  # Pdu5 未在线
    "192.168.1.166",  # Pdu6 未在线
]
# 当前在线的 PDU 行号（0-based），仅这些会通过 SNMP 实时拉取电流
PDU_ONLINE_ROWS = (2,)  # Pdu3
OUTLETS_PER_PDU = 6
SNMP_PORT = 161
SNMP_COMMUNITY = "public"
# 写控制用 private（与 pdu_outlet_switch_test 及手册 snmpset 示例一致）
SNMP_WRITE_COMMUNITY = "private"
# DPDU V2V3-L：电流/功率/状态/控制 OID（与手册及 switch_test 一致）
# 电流：8.1.4.1~6  功率：8.1.5.1~6
# 插座状态（只读）：8.1.2.1~N  1=关闭 2=打开，用于状态灯
# 插座控制（读写）：9.1.2.1~N  1=闭合继电器 2=断开继电器
OID_OUTLET_CURRENT_BASE = "1.3.6.1.4.1.23280.8.1.4"
OID_OUTLET_POWER_BASE = "1.3.6.1.4.1.23280.8.1.5"
OID_OUTLET_STATE_BASE = "1.3.6.1.4.1.23280.8.1.2"
OID_OUTLET_CONTROL_BASE = "1.3.6.1.4.1.23280.9.1.2"
CURRENT_DIVISOR = 100

import asyncio

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
    """与 311.py 一致：v3arch.asyncio GET，返回原始数值。"""
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
    """SNMP V1 GET 单 OID，返回数值或 None。"""
    if not HAS_PYSNMP:
        return None
    return asyncio.run(_snmp_get_async(host, oid, community, port))


async def _snmp_set_async(
    host: str, oid: str, value: int, community: str, port: int
) -> bool:
    """SNMP V1 SET 单 OID 整数值。"""
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
    """SNMP V1 SET 单 OID 整数值，返回是否成功。"""
    if not OID_OUTLET_CONTROL_BASE or not HAS_PYSNMP:
        return False
    return asyncio.run(_snmp_set_async(host, oid, value, community, port))


def read_outlet_current(host: str, outlet_index: int) -> Optional[float]:
    """读取指定插座电流（A）。单位 0.01A → /100=A。"""
    oid = f"{OID_OUTLET_CURRENT_BASE}.{outlet_index}"
    raw = snmp_get(host, oid)
    if raw is None:
        return None
    return round(raw / CURRENT_DIVISOR, 3)


def read_outlet_power(host: str, outlet_index: int) -> Optional[float]:
    """读取指定插座功率（W）。单位 1W → 原始值=W。"""
    oid = f"{OID_OUTLET_POWER_BASE}.{outlet_index}"
    raw = snmp_get(host, oid)
    if raw is None:
        return None
    return round(raw, 1)


def read_pdu_outlets(host: str) -> List[Tuple[Optional[float], Optional[float]]]:
    """读取一台 PDU 的 6 个插座电流与功率。返回 [(电流, 功率), ...]."""
    result = []
    for i in range(1, OUTLETS_PER_PDU + 1):
        cur = read_outlet_current(host, i)
        pwr = read_outlet_power(host, i)
        result.append((cur, pwr))
    return result


# 单次异步拉取一整行（电流+功率+状态），避免主线程卡顿
OutletRowData = List[Tuple[Optional[float], Optional[float], Optional[bool]]]


async def fetch_pdu_row_data_async(ip: str) -> OutletRowData:
    """异步读取一台 PDU 的 6 个插座：电流(A)、功率(W)、状态(开=True)。"""
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
        # 8.1.2 状态：1=关闭 2=打开，状态灯“开”=2
        state = (int(raw_s) == 2) if raw_s is not None else None
        result.append((cur, pwr, state))
    return result


async def fetch_all_online_async() -> Dict[int, OutletRowData]:
    """异步拉取所有在线 PDU 的电流/功率/状态。返回 {行号: [(cur, pwr, state), ...]}."""
    out: Dict[int, OutletRowData] = {}
    for row in PDU_ONLINE_ROWS:
        ip = PDU_IPS[row]
        try:
            out[row] = await fetch_pdu_row_data_async(ip)
        except Exception:
            out[row] = [(None, None, None)] * OUTLETS_PER_PDU
    return out


def set_outlet_on_off(host: str, outlet_index: int, on: bool) -> bool:
    """设置指定插座开(True)=闭合继电器，关(False)=断开继电器。使用 9.1.2.x，1=闭合 2=断开，写 community 为 private。"""
    if not OID_OUTLET_CONTROL_BASE:
        return False
    oid = f"{OID_OUTLET_CONTROL_BASE}.{outlet_index}"
    cmd = 1 if on else 2  # 1=闭合 2=断开（与 pdu_outlet_switch_test 一致）
    return snmp_set(host, oid, cmd, community=SNMP_WRITE_COMMUNITY)


def mock_pdu_outlets(pdu_index: int) -> List[Tuple[Optional[float], Optional[float]]]:
    """无 SNMP 或超时时返回的模拟数据（仅 Pdu3 部分有值）。"""
    if pdu_index == 2:
        return [
            (1.0, 200.0),
            (1.0, 200.0),
            (None, None),
            (1.0, 200.0),
            (None, None),
            (1.0, 200.0),
        ]
    return [(None, None)] * OUTLETS_PER_PDU


class OutletSetWorker(QThread):
    """后台执行插座开/关 SNMP SET，避免主线程卡顿、沙漏。"""
    set_done = Signal(bool, int, int, bool)  # success, row, col, on

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
    """后台线程：拉取在线 PDU 的电流/功率/状态，避免主线程卡顿。"""
    # 不通过信号传 dict（Qt/Shiboken 跨线程无法 copy-convert dict），改为存到 self.result 由主线程读取
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


class PduMonitorWindow(QMainWindow):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("PDU 监控 — 6 台 × 6 插座")
        self.setMinimumSize(1200, 600)
        self._worker_busy = False
        self._worker = None
        self._set_worker = None  # 开/关按钮用的后台线程，避免主线程卡顿
        self._build_ui()
        self._poll_timer = QTimer(self)
        self._poll_timer.timeout.connect(self._refresh_all)
        self._poll_timer.start(3000)
        self._refresh_all()

    def _build_ui(self):
        central = QWidget()
        self.setCentralWidget(central)
        layout = QVBoxLayout(central)

        info = QLabel(
            "型号: v3L | 支持 SNMP V1 / TCP / Telnet / Modbus | "
            "IP 分配 161–166 | 当前连接: 192.168.1.163（Pdu3），Pdu1/2/4/5/6 未在线"
        )
        info.setStyleSheet("color: #666; padding: 4px;")
        layout.addWidget(info)

        group = QGroupBox("插座电流 / 功率")
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
        # 行高缩小，电流/功率行距更紧凑；列宽保证内容可见
        first_col_width = 180
        outlet_col_width = 150
        row_height = 62

        self._table.setColumnWidth(0, first_col_width)
        for col in range(OUTLETS_PER_PDU):
            self._table.setColumnWidth(col + 1, outlet_col_width)

        for row in range(6):
            self._table.setVerticalHeaderItem(row, QTableWidgetItem(f"Pdu{row + 1}"))
            self._table.setRowHeight(row, row_height)

        # 状态灯：长方形（宽x高），用 QFrame 背景色表示绿/红/灰
        STATUS_INDICATOR_W = 28
        STATUS_INDICATOR_H = 16
        # 每个插座一个小 frame，内含：状态灯 + 开/关按钮 + 电流/功率标签
        self._cell_status_labels: List[List[QFrame]] = []
        self._cell_btn_on: List[List[QPushButton]] = []
        self._cell_btn_off: List[List[QPushButton]] = []
        self._cell_current_labels: List[List[QLabel]] = []
        self._cell_power_labels: List[List[QLabel]] = []
        self._outlet_states: List[List[bool]] = []

        for row in range(6):
            self._cell_status_labels.append([])
            self._cell_current_labels.append([])
            self._cell_power_labels.append([])
            self._cell_btn_on.append([])
            self._cell_btn_off.append([])
            self._outlet_states.append([False] * OUTLETS_PER_PDU)
            for col in range(OUTLETS_PER_PDU):
                frame = QWidget()
                frame.setMinimumSize(outlet_col_width - 8, row_height - 6)
                lay = QVBoxLayout(frame)
                lay.setContentsMargins(2, 1, 2, 1)
                lay.setSpacing(0)

                # 顶部：状态长方形 + 开/关按钮
                top = QHBoxLayout()
                status_indicator = QFrame()
                status_indicator.setFixedSize(STATUS_INDICATOR_W, STATUS_INDICATOR_H)
                status_indicator.setStyleSheet("background-color: #c00; border: 1px solid #888; border-radius: 2px;")
                self._cell_status_labels[row].append(status_indicator)
                top.addWidget(status_indicator)

                btn_on = QPushButton("开")
                btn_on.setFixedWidth(32)
                # “开”按钮字体加粗
                font_on = btn_on.font()
                font_on.setBold(True)
                btn_on.setFont(font_on)
                btn_on.clicked.connect(lambda _=False, r=row, c=col: self._on_outlet_control(r, c, True))
                self._cell_btn_on[row].append(btn_on)
                top.addWidget(btn_on)

                btn_off = QPushButton("关")
                btn_off.setFixedWidth(32)
                # “关”按钮字体加粗
                font_off = btn_off.font()
                font_off.setBold(True)
                btn_off.setFont(font_off)
                btn_off.clicked.connect(lambda _=False, r=row, c=col: self._on_outlet_control(r, c, False))
                self._cell_btn_off[row].append(btn_off)
                top.addWidget(btn_off)

                top.addStretch()
                lay.addLayout(top)

                # 底部：电流 / 功率 标签（左对齐）
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

    def _apply_row_data(self, row: int, data: OutletRowData, total_str: str):
        """将一行数据应用到表格：电流、功率、状态灯（由 SNMP 状态决定绿/红）。仅在有变化时更新，减轻界面卡顿。"""
        is_online = row in PDU_ONLINE_ROWS
        ip = PDU_IPS[row]
        status = "在线" if is_online else "未在线"
        # 将“在线/未在线”放到 Pdu 名称后面，保证 PDU frame 3 行：Pdu+状态 / IP / 功率
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
            status_indicator.setStyleSheet(f"background-color: {want}; border: 1px solid #666; border-radius: 2px;")
            # 根据当前状态互斥启用“开/关”按钮：True=闭合(开)→只能点“关”，False=断开(关)→只能点“开”
            if 0 <= row < len(self._cell_btn_on) and 0 <= col < len(self._cell_btn_on[row]):
                btn_on = self._cell_btn_on[row][col]
                btn_off = self._cell_btn_off[row][col]
                if not is_online or state is None:
                    # 未在线或未知状态：两个按钮都禁用，避免误操作
                    btn_on.setEnabled(False)
                    btn_off.setEnabled(False)
                else:
                    # 在线：根据当前状态互斥
                    btn_on.setEnabled(not state)   # 当前断开才允许“开”
                    btn_off.setEnabled(state)      # 当前闭合才允许“关”

    def _on_worker_finished(self):
        """后台线程结束：释放占用标志并解除对 worker 的引用。"""
        self._worker_busy = False
        if self._worker:
            self._worker.deleteLater()
            self._worker = None

    def _on_pdu_data(self):
        """后台拉取完成后更新在线 PDU 行（从 worker.result 读取，避免信号传 dict 报错）。"""
        data_by_row = self._worker.result if self._worker else {}
        for row, data in data_by_row.items():
            if row >= 6 or len(data) != OUTLETS_PER_PDU:
                continue
            total_power = sum((p or 0) for _, p, _ in data)
            total_str = f"{int(total_power)}W" if total_power else "-"
            self._apply_row_data(row, data, total_str)
        self._status.showMessage("实时刷新中（每 3 秒）")

    def _refresh_all(self):
        # 未在线行：直接置空，状态灰
        for row, ip in enumerate(PDU_IPS):
            if row not in PDU_ONLINE_ROWS:
                data = [(None, None, None)] * OUTLETS_PER_PDU
                self._apply_row_data(row, data, "-")

        # 在线行：由后台线程拉取，避免主线程卡顿（保留 worker 引用，防止 QThread 被提前销毁）
        if not self._worker_busy and HAS_PYSNMP and PDU_ONLINE_ROWS:
            self._worker_busy = True
            self._worker = SnmpFetchWorker()
            self._worker.data_ready.connect(self._on_pdu_data)
            self._worker.finished.connect(self._on_worker_finished)
            self._worker.start()

    def _on_set_done(self, success: bool, row: int, col: int, on: bool):
        """插座开/关后台操作完成，更新状态栏。"""
        if self._set_worker:
            self._set_worker.deleteLater()
            self._set_worker = None
        outlet_index = col + 1
        if success:
            self._outlet_states[row][col] = on
            self._status.showMessage(f"Pdu{row + 1} 插座{outlet_index} 已{'开' if on else '关'}")
            # 控制成功后立即更新该插座所在单元格的按钮互斥状态
            try:
                btn_on = self._cell_btn_on[row][col]
                btn_off = self._cell_btn_off[row][col]
                btn_on.setEnabled(not on)
                btn_off.setEnabled(on)
            except Exception:
                # 防御性处理：索引异常时忽略，不影响主流程
                pass
        else:
            self._status.showMessage(f"Pdu{row + 1} 插座{outlet_index} 控制失败（请检查 OID/SNMP 写权限）")

    def _on_outlet_control(self, row: int, col: int, on: bool):
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


def main():
    app = QApplication(sys.argv)
    app.setStyle("Fusion")
    win = PduMonitorWindow()
    win.show()
    sys.exit(app.exec())


if __name__ == "__main__":
    main()
