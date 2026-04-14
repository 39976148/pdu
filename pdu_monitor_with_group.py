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
import queue
import itertools
import threading
import time
from datetime import datetime
from typing import Any, Callable, List, Tuple, Optional, Dict, TypeVar

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
    QCheckBox,
    QMessageBox,
)
from PySide6.QtCore import Qt, QTimer, QThread, Signal, Slot
from PySide6.QtGui import QBrush, QColor

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
# 参与 SNMP 轮询的 PDU 行号（0-based，与 PDU_IPS 下标一致）。此前仅 (2,) 会导致只查 163，其余 IP 永远不轮询。
PDU_ONLINE_ROWS: Tuple[int, ...] = tuple(range(len(PDU_IPS)))
OUTLETS_PER_PDU = 6
SNMP_PORT = 161
SNMP_COMMUNITY = "public"
SNMP_WRITE_COMMUNITY = "private"
OID_OUTLET_CURRENT_BASE = "1.3.6.1.4.1.23280.8.1.4"
OID_OUTLET_POWER_BASE = "1.3.6.1.4.1.23280.8.1.5"
OID_OUTLET_STATE_BASE = "1.3.6.1.4.1.23280.8.1.2"
OID_OUTLET_CONTROL_BASE = "1.3.6.1.4.1.23280.9.1.2"
CURRENT_DIVISOR = 100

# ---- SNMP 性能/兼容性参数（可用环境变量覆盖）----
SNMP_GET_TIMEOUT = float(os.environ.get("PDU_SNMP_TIMEOUT", "0.26"))
SNMP_GET_RETRIES = int(os.environ.get("PDU_SNMP_RETRIES", "0"))
SNMP_SET_TIMEOUT = float(os.environ.get("PDU_SNMP_SET_TIMEOUT", "1.6"))
SNMP_SET_RETRIES = int(os.environ.get("PDU_SNMP_SET_RETRIES", "1"))
SNMP_PROBE_TIMEOUT = float(os.environ.get("PDU_SNMP_PROBE_TIMEOUT", "0.18"))
SNMP_PROBE_RETRIES = int(os.environ.get("PDU_SNMP_PROBE_RETRIES", "0"))
PDU_POLL_MS = max(200, int(os.environ.get("PDU_POLL_MS", "280")))
GROUP_START_INTERVAL_MS = max(500, int(os.environ.get("PDU_GROUP_START_INTERVAL_MS", "3000")))
# 手动开/关：不显示“执行中(灰)”中间态，只以真实状态刷新红/绿
MANUAL_PENDING_MS = max(0, int(os.environ.get("PDU_MANUAL_PENDING_MS", "0")))
# 手动开/关：确认超时（秒）。超时后释放按钮，避免永久禁用
MANUAL_CONFIRM_TIMEOUT_S = float(os.environ.get("PDU_MANUAL_CONFIRM_TIMEOUT_S", "8"))

_SNMP_DEBUG = os.environ.get("PDU_SNMP_DEBUG", "").strip().lower() in ("1", "true", "yes")
_SNMP_SERIAL_FETCH = os.environ.get("PDU_SNMP_SERIAL", "").strip().lower() in ("1", "true", "yes")
_SNMP_IP_PARALLEL = (
    os.environ.get("PDU_SNMP_IP_PARALLEL", "1").strip().lower() not in ("0", "false", "no")
)

# 读状态：哪些整数表示「闭合/ON」
def _parse_env_int_set(key: str, default_csv: str) -> frozenset:
    raw = (os.environ.get(key) or default_csv).strip()
    s: set[int] = set()
    for part in raw.replace(";", ",").split(","):
        part = part.strip()
        if part.lstrip("-").isdigit():
            s.add(int(part))
    if not s:
        for part in default_csv.split(","):
            p = part.strip()
            if p.lstrip("-").isdigit():
                s.add(int(p))
    return frozenset(s)


_STATE_ON_VALUES: frozenset = _parse_env_int_set("PDU_STATE_ON", "2")
PDU_CMD_ON: int = int(os.environ.get("PDU_CMD_ON", "1"))
PDU_CMD_OFF: int = int(os.environ.get("PDU_CMD_OFF", "2"))


def _interpret_outlet_state(raw: Optional[float]) -> Optional[bool]:
    if raw is None:
        return None
    try:
        v = int(round(float(raw)))
    except (TypeError, ValueError):
        return None
    return True if v in _STATE_ON_VALUES else False


# ---- SNMP 单线程调度（避免 Windows 下多线程 pysnmp 原生崩溃 + 让 SET 可插队）----
_T = TypeVar("_T")
_SNMP_SEQ = itertools.count()
_SNMP_PRIORITY_QUEUE: queue.PriorityQueue = queue.PriorityQueue()
_SNMP_WORKER_THREAD: Optional[threading.Thread] = None
_SNMP_WORKER_IDENT: Optional[int] = None
_SNMP_WORKER_READY = threading.Event()
_SNMP_WORKER_START_LOCK = threading.Lock()
_SNMP_STOP = object()


def _snmp_asyncio_run(coro):
    """仅在 SNMP 调度线程内调用。Windows 默认 Proactor 与 UDP/SNMP 组合不稳，强制 Selector。"""
    if sys.platform == "win32":
        policy = asyncio.WindowsSelectorEventLoopPolicy()
    else:
        policy = asyncio.DefaultEventLoopPolicy()
    loop = policy.new_event_loop()
    try:
        return loop.run_until_complete(coro)
    finally:
        try:
            loop.run_until_complete(loop.shutdown_asyncgens())
        except Exception:
            pass
        loop.close()


def _snmp_worker_main() -> None:
    global _SNMP_WORKER_IDENT
    _SNMP_WORKER_IDENT = threading.get_ident()
    _SNMP_WORKER_READY.set()
    while True:
        _prio, _seq, item = _SNMP_PRIORITY_QUEUE.get()
        if item is _SNMP_STOP:
            break
        fn, rq = item
        try:
            rq.put((True, fn()))
        except BaseException as ex:
            rq.put((False, ex))


def _ensure_snmp_worker_started() -> None:
    global _SNMP_WORKER_THREAD
    if _SNMP_WORKER_THREAD is not None and _SNMP_WORKER_THREAD.is_alive():
        return
    with _SNMP_WORKER_START_LOCK:
        if _SNMP_WORKER_THREAD is not None and _SNMP_WORKER_THREAD.is_alive():
            return
        _SNMP_WORKER_READY.clear()
        t = threading.Thread(target=_snmp_worker_main, name="pdu-snmp", daemon=True)
        _SNMP_WORKER_THREAD = t
        t.start()
        _SNMP_WORKER_READY.wait(timeout=15.0)


def _snmp_dispatch(fn: Callable[[], _T], *, urgent: bool = False) -> _T:
    """urgent=True：写指令优先于轮询，减少点击后延迟。"""
    _ensure_snmp_worker_started()
    if threading.get_ident() == _SNMP_WORKER_IDENT:
        return fn()
    rq: queue.Queue = queue.Queue(maxsize=1)
    prio = 0 if urgent else 1
    _SNMP_PRIORITY_QUEUE.put((prio, next(_SNMP_SEQ), (fn, rq)))
    ok, payload = rq.get()
    if not ok:
        raise payload
    return payload

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
            await UdpTransportTarget.create((host, port), timeout=SNMP_GET_TIMEOUT, retries=SNMP_GET_RETRIES),
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
    def _impl() -> Optional[float]:
        return _snmp_asyncio_run(_snmp_get_async(host, oid, community, port))
    try:
        return _snmp_dispatch(_impl, urgent=False)
    except BaseException as ex:
        if _SNMP_DEBUG:
            print(f"[PDU SNMP] snmp_get 异常: {ex!r}", file=sys.stderr)
        return None


async def _snmp_set_async(host: str, oid: str, value: int, community: str, port: int) -> bool:
    if not HAS_PYSNMP or set_cmd is None or Integer32 is None:
        return False
    engine = SnmpEngine()
    try:
        result = await set_cmd(
            engine,
            CommunityData(community, mpModel=0),
            await UdpTransportTarget.create((host, port), timeout=SNMP_SET_TIMEOUT, retries=SNMP_SET_RETRIES),
            ContextData(),
            ObjectType(ObjectIdentity(oid), Integer32(value)),
        )
        err_ind, err_status, _, _ = result
        ok = not (err_ind or err_status)
        if not ok and _SNMP_DEBUG:
            print(
                f"[PDU SNMP] SET {host}:{port} {oid} value={value} community={community!r} "
                f"err_ind={err_ind!r} err_status={err_status!r}",
                file=sys.stderr,
            )
        return ok
    except Exception:
        return False
    finally:
        engine.close_dispatcher()


def snmp_set(host: str, oid: str, value: int, community: str = SNMP_COMMUNITY, port: int = SNMP_PORT) -> bool:
    if not OID_OUTLET_CONTROL_BASE or not HAS_PYSNMP:
        return False
    def _impl() -> bool:
        return bool(_snmp_asyncio_run(_snmp_set_async(host, oid, value, community, port)))
    try:
        return bool(_snmp_dispatch(_impl, urgent=True))
    except BaseException as ex:
        if _SNMP_DEBUG:
            print(f"[PDU SNMP] snmp_set 异常: {ex!r}", file=sys.stderr)
        return False


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
    # 探活：失败则整行离线，避免离线 IP 打满 18 次 GET
    probe_oid = f"{OID_OUTLET_CURRENT_BASE}.1"
    if (
        await _snmp_get_async(ip, probe_oid, SNMP_COMMUNITY, SNMP_PORT)
        is None
    ):
        return [(None, None, None)] * OUTLETS_PER_PDU
    result: OutletRowData = []
    for i in range(1, OUTLETS_PER_PDU + 1):
        oid_c = f"{OID_OUTLET_CURRENT_BASE}.{i}"
        oid_p = f"{OID_OUTLET_POWER_BASE}.{i}"
        oid_s = f"{OID_OUTLET_STATE_BASE}.{i}"
        raw_c, raw_p, raw_s = await asyncio.gather(
            _snmp_get_async(ip, oid_c, SNMP_COMMUNITY, SNMP_PORT),
            _snmp_get_async(ip, oid_p, SNMP_COMMUNITY, SNMP_PORT),
            _snmp_get_async(ip, oid_s, SNMP_COMMUNITY, SNMP_PORT),
        )
        cur = round(raw_c / CURRENT_DIVISOR, 3) if raw_c is not None else None
        pwr = round(raw_p, 1) if raw_p is not None else None
        state = _interpret_outlet_state(raw_s)
        result.append((cur, pwr, state))
    return result


async def fetch_all_online_async() -> Dict[int, OutletRowData]:
    rows = [r for r in PDU_ONLINE_ROWS if 0 <= r < len(PDU_IPS)]
    if not rows:
        return {}

    async def _fetch_one(row: int) -> Tuple[int, OutletRowData]:
        ip = PDU_IPS[row]
        try:
            return row, await fetch_pdu_row_data_async(ip)
        except Exception:
            return row, [(None, None, None)] * OUTLETS_PER_PDU

    if _SNMP_SERIAL_FETCH or not _SNMP_IP_PARALLEL:
        out: Dict[int, OutletRowData] = {}
        for row in rows:
            r, data = await _fetch_one(row)
            out[r] = data
        return out
    pairs = await asyncio.gather(*(_fetch_one(r) for r in rows))
    return dict(pairs)


def set_outlet_on_off(host: str, outlet_index: int, on: bool) -> bool:
    if not OID_OUTLET_CONTROL_BASE:
        return False
    oid = f"{OID_OUTLET_CONTROL_BASE}.{outlet_index}"
    cmd = PDU_CMD_ON if on else PDU_CMD_OFF
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
    """后台一次性并行断开所有插座（关键口除外）。"""
    finished_all = Signal()

    def __init__(self, rows: List[int], protected: Optional[set] = None):
        super().__init__()
        self._rows = rows  # PDU_ONLINE_ROWS
        self._protected = protected or set()

    def run(self):
        try:
            def _impl() -> None:
                # 这里不要把所有 set_cmd 全部并发 gather：在部分设备/Windows+pysnmp 组合下会触发原生崩溃。
                # 改为「限并发」(默认 1) 发送，确保稳定；需要更快可设 PDU_ALL_OFF_CONCURRENCY=2/3。
                concurrency = max(1, int(os.environ.get("PDU_ALL_OFF_CONCURRENCY", "1")))

                async def _do_all() -> None:
                    sem = asyncio.Semaphore(concurrency)

                    async def _one(ip: str, oid: str) -> None:
                        async with sem:
                            await _snmp_set_async(
                                ip, oid, PDU_CMD_OFF, SNMP_WRITE_COMMUNITY, SNMP_PORT
                            )

                    tasks = []
                    for row in self._rows:
                        if row < 0 or row >= len(PDU_IPS):
                            continue
                        ip = PDU_IPS[row]
                        for outlet_index in range(1, OUTLETS_PER_PDU + 1):
                            col = outlet_index - 1
                            if (row, col) in self._protected:
                                continue
                            oid = f"{OID_OUTLET_CONTROL_BASE}.{outlet_index}"
                            tasks.append(asyncio.create_task(_one(ip, oid)))
                    if tasks:
                        await asyncio.gather(*tasks, return_exceptions=True)

                _snmp_asyncio_run(_do_all())
            _snmp_dispatch(_impl, urgent=True)
        finally:
            self.finished_all.emit()


class SnmpFetchWorker(QThread):
    data_ready = Signal(object)

    def __init__(self):
        super().__init__()
        self.result: Dict[int, OutletRowData] = {}

    def run(self):
        res: Dict[int, OutletRowData] = {}
        try:
            if HAS_PYSNMP and PDU_ONLINE_ROWS:
                def _impl() -> Dict[int, OutletRowData]:
                    return _snmp_asyncio_run(fetch_all_online_async())
                res = _snmp_dispatch(_impl, urgent=False)
        except Exception as ex:
            if _SNMP_DEBUG:
                import traceback
                print(f"[PDU SNMP] SnmpFetchWorker.run 异常: {ex!r}", file=sys.stderr)
                traceback.print_exc()
            res = {}
        finally:
            self.result = res
            self.data_ready.emit(res)


class PduMonitorWithGroupWindow(QMainWindow):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("PDU 监控（带分组开机）— 6 台 × 6 插座")
        self.setMinimumSize(1300, 650)
        self._worker_busy = False
        self._worker = None
        # 手动开/关：允许快速连点多路，每路一个 QThread；SNMP 经调度队列依次执行
        self._manual_set_workers: List[OutletSetWorker] = []
        self._all_off_worker = None  # 全部断开后台线程
        self._all_off_in_progress = False
        self._online_rows_runtime: set[int] = set()
        # 手动开/关 pending：用于“超过阈值才显示灰色执行中”，以及失败回滚
        self._manual_pending: Dict[Tuple[int, int], dict] = {}

        self._group_start_buttons: Dict[str, QPushButton] = {}
        self._group_set_workers: List[OutletSetWorker] = []
        self._group_start_busy = False
        self._group_ops_remaining = 0

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
        self._poll_timer.start(PDU_POLL_MS)
        self._refresh_all()

        # CSV 定时写入：与 SNMP 刷新解耦，保证 1s 一个 record（写入的是“最近一次已知快照”）
        self._csv_timer = QTimer(self)
        self._csv_timer.timeout.connect(self._save_csv_auto)
        self._csv_timer.start(1000)

    def _update_total_power_header(self) -> None:
        """更新表头第 0 列：显示 6 个 PDU 的总功率合计。"""
        try:
            n_rows = min(6, len(PDU_IPS))
            total_sum = 0.0
            has_any = False
            for r in range(n_rows):
                if r not in self._online_rows_runtime:
                    continue
                data = self._last_data_by_row.get(r)
                if not isinstance(data, list) or len(data) != OUTLETS_PER_PDU:
                    continue
                total_sum += float(sum((p or 0.0) for _, p, _ in data))
                has_any = True
            label = "PDU / 总功率"
            if has_any:
                label = f"PDU / 总功率（合计 {int(round(total_sum))}W）"
            self._table.setHorizontalHeaderLabels(
                [label] + [f"插座{i}" for i in range(1, OUTLETS_PER_PDU + 1)]
            )
        except Exception:
            pass

    def _build_ui(self):
        central = QWidget()
        self.setCentralWidget(central)
        layout = QVBoxLayout(central)

        self._info_label = QLabel()
        self._info_label.setWordWrap(True)
        self._info_label.setStyleSheet("color: #666; padding: 4px;")
        layout.addWidget(self._info_label)
        self._update_top_info_banner()

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
        # 关键/保护插座：禁止断开，且“全部断开”跳过
        self._cell_protected: List[List["QCheckBox"]] = []
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
            self._cell_protected.append([])
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
                # 最上方一行：别名 + 关键口
                top0 = QHBoxLayout()
                top0.setContentsMargins(0, 0, 0, 0)
                top0.setSpacing(4)
                top0.addWidget(alias_edit, 1)

                protected_cb = QCheckBox("关键")
                protected_cb.setToolTip("关键设备供电口：禁止断开；“全部断开”会自动跳过")
                protected_cb.stateChanged.connect(self._save_aliases)
                self._cell_protected[row].append(protected_cb)
                top0.addWidget(protected_cb)
                lay.addLayout(top0)

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
        busy = getattr(self, "_group_start_busy", False)
        for ch, btn in self._group_start_buttons.items():
            btn.setEnabled((ch in groups and len(groups[ch]) > 0) and not busy)

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

        # 恢复关键口（protected）
        protected = data.get("outlet_protected") or {}
        for row in range(6):
            for col in range(OUTLETS_PER_PDU):
                if row >= len(self._cell_protected) or col >= len(self._cell_protected[row]):
                    continue
                key = f"{row}_{col}"
                is_prot = bool(protected.get(key)) if isinstance(protected, dict) else False
                try:
                    self._cell_protected[row][col].blockSignals(True)
                    self._cell_protected[row][col].setChecked(is_prot)
                    self._cell_protected[row][col].blockSignals(False)
                except Exception:
                    pass
                # 需求：关键口启动时默认视为“闭合”（用于组开机过滤等逻辑）
                if is_prot and row < len(self._outlet_states) and col < len(self._outlet_states[row]):
                    self._outlet_states[row][col] = True
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
        outlet_protected = {}
        for row in range(len(self._cell_protected)):
            for col in range(len(self._cell_protected[row])):
                try:
                    if self._cell_protected[row][col].isChecked():
                        outlet_protected[f"{row}_{col}"] = True
                except Exception:
                    pass
        group = {}
        for ch in "ABCDEFGHIJ":
            text = self._group_alias_edits[ch].text().strip()
            if text:
                group[ch] = text
        now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        payload = {
            "outlet_alias": outlet,
            "outlet_group": outlet_group,
            "outlet_protected": outlet_protected,
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
            # 不刷屏状态栏（避免与控制/刷新提示互相覆盖）
            pass

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

    def _update_top_info_banner(self) -> None:
        """顶部说明随本轮在线状态刷新（不再写死某一台 IP，也不罗列全部目标）。"""
        n = len(PDU_IPS)
        online_sorted = sorted(self._online_rows_runtime)
        if online_sorted:
            online_parts = [f"Pdu{r + 1}（{PDU_IPS[r]}）" for r in online_sorted if 0 <= r < n]
            online_str = "，".join(online_parts)
        else:
            online_str = "无（请检查网络、SNMP 团体字与防火墙 UDP/161）"
        self._info_label.setText(
            f"PDU 监控（带分组） | 型号 v3L | SNMP v1 | UDP/{SNMP_PORT} 只读 {SNMP_COMMUNITY} | 当前在线: {online_str}"
        )

    # ---- SNMP 拉取与刷新 ----
    def _apply_row_data(self, row: int, data: OutletRowData, total_str: str, is_online: Optional[bool] = None):
        if is_online is None:
            is_online = row in self._online_rows_runtime
        ip = PDU_IPS[row]
        status = "在线" if is_online else "未在线"
        pdu_label = f"Pdu{row + 1} {status}\n{ip}"
        item0 = QTableWidgetItem(f"{pdu_label}\n功率 {total_str}")
        if not is_online:
            item0.setBackground(QBrush(QColor("#e9ecef")))
        self._table.setItem(row, 0, item0)
        self._last_data_by_row[row] = list(data)  # 供保存 CSV 使用
        for col, item in enumerate(data):
            if col >= len(self._cell_current_labels[row]):
                break
            cur, pwr, state = item if len(item) >= 3 else (item[0], item[1], None)
            cur_text = f"{cur:.3f}A" if cur is not None else "-A"
            # 需求：在线时功率应显示 0；未在线时显示 -
            if is_online and pwr is None:
                pwr = 0.0
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
            pend = self._manual_pending.get((row, col))
            if state is not None and row < len(self._outlet_states) and col < len(self._outlet_states[row]):
                self._outlet_states[row][col] = state
                # 仅当读回状态 == 目标状态，才结束 pending；否则继续禁用按钮等待下一轮确认
                if pend is not None:
                    desired = pend.get("desired")
                    if desired is None:
                        # 兼容旧结构：无 desired 时不自动结束 pending
                        pass
                    elif state is desired:
                        self._manual_pending.pop((row, col), None)
            if 0 <= row < len(self._cell_btn_on) and 0 <= col < len(self._cell_btn_on[row]):
                btn_on = self._cell_btn_on[row][col]
                btn_off = self._cell_btn_off[row][col]
                # 手动开/关指令进行中：禁止被轮询用旧状态重新启用按钮
                pend = self._manual_pending.get((row, col))
                if pend is not None:
                    started_at = float(pend.get("started_at") or 0.0)
                    if started_at > 0 and (time.time() - started_at) > MANUAL_CONFIRM_TIMEOUT_S:
                        # 超时：释放 pending（不改变指示灯颜色，保持真实轮询结果）
                        self._manual_pending.pop((row, col), None)
                    else:
                        btn_on.setEnabled(False)
                        btn_off.setEnabled(False)
                        continue
                protected = False
                try:
                    protected = self._cell_protected[row][col].isChecked()
                except Exception:
                    protected = False
                if protected:
                    btn_off.setEnabled(False)
                    if not is_online or state is None:
                        btn_on.setEnabled(False)
                    else:
                        btn_on.setEnabled(not state)
                elif not is_online or state is None:
                    btn_on.setEnabled(False)
                    btn_off.setEnabled(False)
                else:
                    btn_on.setEnabled(not state)
                    btn_off.setEnabled(state)

        # 每次刷完一行后，整体刷新一下组按钮可用状态
        self._refresh_group_buttons_enabled()

    @Slot()
    def _on_snmp_fetch_worker_finished(self) -> None:
        self._worker_busy = False
        w = self._worker
        self._worker = None
        if w is not None:
            w.deleteLater()

    @Slot(object)
    def _on_pdu_data(self, data_by_row: object) -> None:
        if getattr(self, "_all_off_in_progress", False):
            return
        if not isinstance(data_by_row, dict):
            data_by_row = {}
        n_rows = min(6, len(PDU_IPS))
        if not data_by_row:
            for row in PDU_ONLINE_ROWS:
                if row >= n_rows:
                    continue
                self._online_rows_runtime.discard(row)
                dead = [(None, None, None)] * OUTLETS_PER_PDU
                self._apply_row_data(row, dead, "-", is_online=False)
            self._update_top_info_banner()
            self._update_total_power_header()
            return
        for row, data in data_by_row.items():
            if row >= n_rows or len(data) != OUTLETS_PER_PDU:
                continue
            has_valid = any((c is not None) or (p is not None) or (s is not None) for c, p, s in data)
            if has_valid:
                self._online_rows_runtime.add(row)
            else:
                self._online_rows_runtime.discard(row)
            total_power = sum((p or 0) for _, p, _ in data)
            total_str = f"{int(round(total_power))}W" if has_valid else "-"
            self._apply_row_data(row, data, total_str, is_online=has_valid)
        self._update_top_info_banner()
        self._update_total_power_header()
        # CSV 由独立 1s 定时器写入最近快照

    def _refresh_all(self):
        for row, ip in enumerate(PDU_IPS):
            if row not in PDU_ONLINE_ROWS:
                data = [(None, None, None)] * OUTLETS_PER_PDU
                self._apply_row_data(row, data, "-", is_online=False)
        # 对尚未确认在线的行，用离线占位；已在线行保持上一轮数据避免闪烁
        if not self._worker_busy:
            for row in PDU_ONLINE_ROWS:
                if row >= len(PDU_IPS):
                    continue
                if row in self._online_rows_runtime:
                    continue
                data = [(None, None, None)] * OUTLETS_PER_PDU
                self._apply_row_data(row, data, "-", is_online=False)

        if not self._worker_busy and HAS_PYSNMP and PDU_ONLINE_ROWS and not getattr(self, "_all_off_in_progress", False):
            self._worker_busy = True
            self._worker = SnmpFetchWorker()
            self._worker.data_ready.connect(self._on_pdu_data, Qt.ConnectionType.QueuedConnection)
            self._worker.finished.connect(self._on_snmp_fetch_worker_finished, Qt.ConnectionType.QueuedConnection)
            self._worker.start()
        else:
            self._update_top_info_banner()

    def _paint_outlet_closed_ui(self, row: int, col: int, closed: bool) -> None:
        """closed=True 表示闭合/开。"""
        self._outlet_states[row][col] = closed
        try:
            status_indicator = self._cell_status_labels[row][col]
            want = "#0a0" if closed else "#c00"
            status_indicator.setStyleSheet(
                f"background-color: {want}; border: 1px solid #666; border-radius: 2px;"
            )
            self._cell_btn_on[row][col].setEnabled(not closed)
            self._cell_btn_off[row][col].setEnabled(closed)
        except Exception:
            pass

    def _mark_manual_pending_if_needed(self, row: int, col: int, token: object) -> None:
        """仅当该口仍处于本次手动操作 pending 时才置灰。"""
        try:
            pend = self._manual_pending.get((row, col))
            if not pend or pend.get("token") is not token:
                return
            # 仍未读回真实状态：置为灰色“执行中”
            self._cell_status_labels[row][col].setStyleSheet(
                "background-color: #888; border: 1px solid #666; border-radius: 2px;"
            )
        except Exception:
            pass

    def _apply_manual_set_result(self, success: bool, row: int, col: int, on: bool) -> None:
        outlet_index = col + 1
        if success:
            # 成功：此处可以更新红/绿，但更可靠的是立刻触发一次刷新读回真实状态
            self._status.showMessage(f"Pdu{row + 1} 插座{outlet_index} 写入成功，正在确认状态…")
            self._refresh_all()
        else:
            prev = self._manual_pending.get((row, col), {}).get("prev")
            if prev is None:
                prev = self._outlet_states[row][col] if row < len(self._outlet_states) else False
            self._paint_outlet_closed_ui(row, col, bool(prev))
            self._manual_pending.pop((row, col), None)
            self._status.showMessage(f"Pdu{row + 1} 插座{outlet_index} 控制失败，界面已回滚")

    def _on_outlet_control(self, row: int, col: int, on: bool):
        """单个插座手控开/关：允许快速连点多路，SNMP 在调度队列中依次执行；红/绿指示仅在真实确认后更新。"""
        if not on:
            try:
                if self._cell_protected[row][col].isChecked():
                    self._status.showMessage(f"Pdu{row + 1} 插座{col + 1} 为关键供电口：禁止断开")
                    return
            except Exception:
                pass
        ip = PDU_IPS[row]
        if row not in self._online_rows_runtime:
            self._status.showMessage(f"Pdu{row + 1} 未在线，无法控制")
            return
        outlet_index = col + 1
        prev = self._outlet_states[row][col] if row < len(self._outlet_states) else False
        token = object()
        self._manual_pending[(row, col)] = {
            "prev": prev,
            "token": token,
            "desired": bool(on),
            "started_at": time.time(),
        }

        # 点击后不动红/绿，只禁用按钮；红/绿仅在读回真实状态后更新
        try:
            self._cell_btn_on[row][col].setEnabled(False)
            self._cell_btn_off[row][col].setEnabled(False)
        except Exception:
            pass
        self._status.showMessage(f"正在下发 Pdu{row + 1} 插座{outlet_index} {'闭合' if on else '断开'}…")

        worker = OutletSetWorker(ip, outlet_index, on, row, col)
        self._manual_set_workers.append(worker)

        def _on_done(success: bool, r: int, c: int, o: bool):
            try:
                self._manual_set_workers.remove(worker)
            except ValueError:
                pass
            self._apply_manual_set_result(success, r, c, o)
            worker.deleteLater()

        worker.set_done.connect(_on_done, Qt.ConnectionType.QueuedConnection)
        worker.start()

    def _on_all_off_clicked(self):
        """全部断开：先确认，再断开所有在线 PDU 的全部插座。"""
        online_rows = sorted(self._online_rows_runtime)
        if not online_rows:
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

        self._all_off_in_progress = True
        protected = set()
        for r in range(6):
            for c in range(OUTLETS_PER_PDU):
                try:
                    if self._cell_protected[r][c].isChecked():
                        protected.add((r, c))
                except Exception:
                    pass

        # UI 立即响应：先把非关键口标为断开，避免轮询排队造成“先红后绿再红”的错觉
        for r in online_rows:
            if r < 0 or r >= len(self._outlet_states):
                continue
            for c in range(OUTLETS_PER_PDU):
                if (r, c) in protected:
                    continue
                self._paint_outlet_closed_ui(r, c, False)

        self._all_off_worker = AllOffWorker(online_rows, protected=protected)
        self._all_off_worker.finished_all.connect(self._on_all_off_done)
        self._all_off_worker.start()
        if protected:
            self._status.showMessage(f"正在断开全部插座…（已跳过关键口 {len(protected)} 个）")
        else:
            self._status.showMessage("正在断开全部插座…")

    def _on_all_off_done(self):
        """全部断开后台完成：刷新界面、释放 worker 并弹出完成提示。"""
        if self._all_off_worker:
            self._all_off_worker.deleteLater()
            self._all_off_worker = None
        self._all_off_in_progress = False
        self._refresh_all()
        self._status.showMessage("全部插座已断开")
        # 不弹窗，避免阻塞主线程

    def _group_op_finished(self) -> None:
        """组开机序列中每步结束（含跳过）时调用。"""
        if self._group_ops_remaining > 0:
            self._group_ops_remaining -= 1
            if self._group_ops_remaining <= 0:
                self._group_ops_remaining = 0
                self._group_start_busy = False
        self._refresh_group_buttons_enabled()

    def _group_on_outlet_control(self, row: int, col: int, on: bool):
        """组开机用的控制：可多路 worker；SNMP 仍单队列串行。"""
        ip = PDU_IPS[row]
        if row not in self._online_rows_runtime:
            self._group_op_finished()
            return
        outlet_index = col + 1

        worker = OutletSetWorker(ip, outlet_index, on, row, col)
        self._group_set_workers.append(worker)

        def _on_done(success: bool, r: int, c: int, o: bool):
            try:
                self._group_set_workers.remove(worker)
            except ValueError:
                pass
            outlet_idx = c + 1
            if success:
                self._paint_outlet_closed_ui(r, c, o)
                self._status.showMessage(f"[组开机] Pdu{r + 1} 插座{outlet_idx} 已{'闭合' if o else '断开'}")
            else:
                self._status.showMessage(f"[组开机] Pdu{r + 1} 插座{outlet_idx} 控制失败（SNMP 写或设备忙）")
            worker.deleteLater()
            self._group_op_finished()

        worker.set_done.connect(_on_done, Qt.ConnectionType.QueuedConnection)
        worker.start()

    # ---- 组开机逻辑：先查状态，仅对当前断开的插座按序号顺序、间隔 GROUP_START_INTERVAL_MS 发送闭合 ----
    def _on_group_start(self, group_letter: str):
        if self._group_start_busy:
            self._status.showMessage("组开机序列进行中，请等待当前批次完成后再操作")
            return
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

        self._group_start_busy = True
        self._group_ops_remaining = len(need_close)
        self._refresh_group_buttons_enabled()

        interval_ms = GROUP_START_INTERVAL_MS
        for step_idx, (_, row, col) in enumerate(need_close):
            delay = step_idx * interval_ms
            QTimer.singleShot(
                delay,
                lambda r=row, c=col: self._group_on_outlet_control(r, c, True),
            )

        names = ", ".join(f"Pdu{r + 1}-插座{c + 1}#{order}" for order, r, c in need_close)
        self._status.showMessage(
            f"已启动 {group_letter} 组开机（仅对当前断开的插座，间隔 {interval_ms / 1000:g} 秒）：{names}"
        )


def main():
    app = QApplication(sys.argv)
    app.setStyle("Fusion")
    win = PduMonitorWithGroupWindow()
    win.show()
    sys.exit(app.exec())


if __name__ == "__main__":
    main()

