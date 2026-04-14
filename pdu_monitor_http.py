# -*- coding: utf-8 -*-
"""
PDU 监控面板（HTTP API 版，带分组开机功能）

目标：用 HTTP API（见 http-api-pdu_gwgj-demo.py）复刻 pdu_monitor_with_group.py 的功能：
- 6 台 × 6 插座表格
- 单口开/关（真实确认后更新红/绿；pending 期间禁用按钮）
- 全部断开（关键口除外）
- 分组开机（A–J + 0–9 顺序，默认 3 秒间隔）
- 别名/分组/关键口保存与下次启动载入
- CSV 1Hz 记录（写入“最近一次快照”，与轮询解耦）

说明（按文档）：
- GET  http://{ip}/api/pdu/0/device-status   获取总数据（含 active_power(kW) 等）
- GET  http://{ip}/api/pdu/0/outlets-status  获取各插座数据（含 current(A)、power(kW)、state 等）
- POST http://{ip}/api/pdu/0/outlet-control  控制插座；slot_no 使用十进制位掩码：
  - 单口：1/2/4/8/16/32/64/128（对应第 1..8 口）
  - 多口组合：可相加，例如控制 1+2 口 slot_no=3
- 功率单位：文档多为 kW，部分固件插座字段实际为 W。默认 auto（数值≥50 视为 W，否则按 kW×1000）。
  若显示仍不对，可设环境变量 PDU_HTTP_POWER_UNIT=w 或 kw（亦支持 PDU_HTTP_OUTLET_POWER_UNIT）。
- 部分机型 JSON 无电流/功率，可启用网页补数（需 admin 登录；实时量来自 CGI，与浏览器一致）：
  - PDU_HTML_METRICS=1
  - PDU_HTML_USER / PDU_HTML_PASSWORD（默认 admin/admin）
  - 内部顺序：GET /switch_data_Onceload.cgi → GET/POST /switch_control_Upload.cgi（分号分隔）
  - 可选 pip install beautifulsoup4（仅作静态 HTML 回退）
- 性能：多台 PDU 并行拉取（环境变量 PDU_HTTP_FETCH_WORKERS，默认 8）；开关成功后对该行优先刷新，不排队等整轮轮询
- 全部断开：同一 IP 串行 POST + 口间间隔（PDU_HTTP_ALL_OFF_STEP_S）；多台 PDU 并行数 PDU_HTTP_ALL_OFF_IP_PARALLEL；失败重试 PDU_HTTP_ALL_OFF_PER_OUTLET_RETRY
"""

from __future__ import annotations

import csv
import json
import os
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

import requests
import threading

from PySide6.QtCore import Qt, QTimer, QThread, Signal, Slot
from PySide6.QtGui import QBrush, QColor, QCloseEvent
from PySide6.QtWidgets import (
    QApplication,
    QCheckBox,
    QComboBox,
    QFrame,
    QGroupBox,
    QHBoxLayout,
    QHeaderView,
    QLabel,
    QLineEdit,
    QMainWindow,
    QMessageBox,
    QPushButton,
    QStatusBar,
    QTableWidget,
    QTableWidgetItem,
    QVBoxLayout,
    QWidget,
)


# -------------------- 基本配置（可用环境变量覆盖） --------------------
PDU_IPS: List[str] = json.loads(os.environ.get("PDU_IPS_JSON", "[]") or "[]") or [
    "192.168.1.161",
    "192.168.1.162",
    "192.168.1.163",
    "192.168.1.164",
    "192.168.1.165",
    "192.168.1.166",
]
PDU_ONLINE_ROWS: Tuple[int, ...] = tuple(range(len(PDU_IPS)))
OUTLETS_PER_PDU = 6

HTTP_TIMEOUT_S = float(os.environ.get("PDU_HTTP_TIMEOUT", "0.6"))
HTTP_RETRIES = int(os.environ.get("PDU_HTTP_RETRIES", "0"))
PDU_POLL_MS = max(200, int(os.environ.get("PDU_POLL_MS", "350")))
GROUP_START_INTERVAL_MS = max(500, int(os.environ.get("PDU_GROUP_START_INTERVAL_MS", "3000")))
MANUAL_CONFIRM_TIMEOUT_S = float(os.environ.get("PDU_MANUAL_CONFIRM_TIMEOUT_S", "8"))

# 全部断开：同一台 PDU 上必须串行（并发 POST 易被固件丢包）；多台 PDU 之间可并行
PDU_ALL_OFF_IP_PARALLEL = max(1, int(os.environ.get("PDU_HTTP_ALL_OFF_IP_PARALLEL", "4")))
PDU_ALL_OFF_STEP_S = float(os.environ.get("PDU_HTTP_ALL_OFF_STEP_S", "0.08"))
PDU_ALL_OFF_PER_OUTLET_RETRY = max(0, int(os.environ.get("PDU_HTTP_ALL_OFF_PER_OUTLET_RETRY", "2")))

# HTML/CGI 计量缓存：避免每次轮询都打 CGI（页面本身 4s 更新一次）
_HTML_METRICS_CACHE_LOCK = threading.Lock()
_HTML_METRICS_CACHE: Dict[str, Dict[str, Any]] = {}
HTML_METRICS_CACHE_TTL_S = float(os.environ.get("PDU_HTML_CACHE_TTL_S", "2.0"))
# 并行拉取多台 PDU（原串行时 6 台×数秒 CGI ≈ 30s 才刷满一屏）
PDU_HTTP_FETCH_WORKERS = max(1, int(os.environ.get("PDU_HTTP_FETCH_WORKERS", "8")))


OutletRowData = List[Tuple[Optional[float], Optional[float], Optional[bool]]]


def _slotno_to_port(slot_no: Any) -> Optional[int]:
    """兼容两种固件返回：
    1) 顺序号：1..N（文档 outlets-status 示例是这种）
    2) 位掩码：1/2/4/8/...（旧 demo 与 outlet-control 使用这种）
    """
    try:
        v = int(slot_no)
    except Exception:
        return None
    if v <= 0:
        return None
    # 顺序号（1..N）
    if 1 <= v <= OUTLETS_PER_PDU:
        return v
    # 位掩码（必须是 2 的幂）
    if v & (v - 1) == 0:
        port = (v.bit_length() - 1) + 1
        return port
    return None


def _port_to_slotno(port: int) -> int:
    return 2 ** (int(port) - 1)


def _parse_state_to_bool(s: Any) -> Optional[bool]:
    if s is None:
        return None
    if isinstance(s, bool):
        return s
    text = str(s).strip().upper()
    if text in ("ON", "1", "TRUE", "CLOSED", "CLOSE"):
        return True
    if text in ("OFF", "0", "FALSE", "OPEN", "OPENED"):
        return False
    return None


def _extract_float(d: Dict[str, Any], keys: List[str]) -> Optional[float]:
    for k in keys:
        if k in d:
            try:
                return float(d[k])
            except Exception:
                continue
    return None


def _dict_get_ci(d: Dict[str, Any], key: str) -> Any:
    """大小写不敏感取键（固件可能返回 Power / CURRENT 等）。"""
    lk = str(key).lower()
    for k in d.keys():
        if str(k).lower() == lk:
            return d[k]
    return None


def _parse_float_loose(v: Any) -> Optional[float]:
    if v is None:
        return None
    if isinstance(v, (int, float)):
        return float(v)
    s = str(v).strip().replace(",", "")
    if not s:
        return None
    # 去掉常见单位后缀
    for suf in ("kw", "kwh", "w", "wh", "a", "ma", "v", "hz"):
        if s.lower().endswith(suf):
            s = s[: -len(suf)].strip()
            break
    try:
        return float(s)
    except Exception:
        return None


def _extract_float_ci(d: Dict[str, Any], keys: List[str]) -> Optional[float]:
    for k in keys:
        v = _dict_get_ci(d, k)
        if v is None:
            continue
        x = _parse_float_loose(v)
        if x is not None:
            return x
    return None


def _power_raw_to_w(raw: Optional[float]) -> Optional[float]:
    """API 文档多为 kW；部分固件插座/总功率字段实际为 W。统一换算为瓦特显示。
    环境变量（优先前者）：PDU_HTTP_OUTLET_POWER_UNIT 或 PDU_HTTP_POWER_UNIT，取值 kw / w / auto（默认 auto）。
    """
    if raw is None:
        return None
    mode = (
        os.environ.get("PDU_HTTP_OUTLET_POWER_UNIT") or os.environ.get("PDU_HTTP_POWER_UNIT") or "auto"
    ).strip().lower()
    if mode in ("w", "watt", "watts"):
        return raw
    if mode in ("kw", "kilowatt", "k"):
        return raw * 1000.0
    # auto：较大数值按 W（单口/整机若以 kW 书写通常 < 几十）
    if raw >= 50.0:
        return raw
    return raw * 1000.0


class PDUHttpClient:
    def __init__(self, ip: str):
        self.ip = ip
        self.base = f"http://{ip}/api/pdu/0/"
        self.session = requests.Session()

    def get_outlets_status(self) -> Dict[str, Any]:
        url = self.base + "outlets-status"
        last_ex: Optional[Exception] = None
        for _ in range(max(1, HTTP_RETRIES + 1)):
            try:
                r = self.session.get(url, timeout=HTTP_TIMEOUT_S)
                if r.status_code != 200:
                    return {"error": f"HTTP {r.status_code}"}
                return r.json()
            except Exception as ex:
                last_ex = ex
        return {"error": f"{last_ex!r}"}

    def get_device_status(self) -> Dict[str, Any]:
        url = self.base + "device-status"
        last_ex: Optional[Exception] = None
        for _ in range(max(1, HTTP_RETRIES + 1)):
            try:
                r = self.session.get(url, timeout=HTTP_TIMEOUT_S)
                if r.status_code != 200:
                    return {"error": f"HTTP {r.status_code}"}
                return r.json()
            except Exception as ex:
                last_ex = ex
        return {"error": f"{last_ex!r}"}

    def set_outlet(self, port: int, on: bool) -> bool:
        url = self.base + "outlet-control"
        payload = {"slot_no": _port_to_slotno(port), "active": ("ON" if on else "OFF")}
        last_ex: Optional[Exception] = None
        for _ in range(max(1, HTTP_RETRIES + 1)):
            try:
                r = self.session.post(url, json=payload, timeout=HTTP_TIMEOUT_S)
                if r.status_code != 200:
                    last_ex = RuntimeError(f"HTTP {r.status_code}")
                    continue
                data = r.json()
                # 文档：code==200 表示成功；500/400 为失败
                if isinstance(data, dict):
                    if data.get("code") == 200:
                        return True
                    if data.get("code") in (400, 500):
                        return False
                    # 少数固件可能只返回 {"message":"OK"}
                    msg = str(data.get("message") or "").strip().upper()
                    if msg == "OK":
                        return True
                return False
            except Exception as ex:
                last_ex = ex
        return False


def fetch_pdu_row_data_http(ip: str) -> Tuple[OutletRowData, Optional[float]]:
    cli = PDUHttpClient(ip)
    total_w: Optional[float] = None

    dev = cli.get_device_status()
    if isinstance(dev, dict) and not dev.get("error") and isinstance(dev.get("data"), dict):
        d = dev["data"]
        ap = _extract_float_ci(d, ["active_power", "total_power", "total_active_power", "power"])
        total_w = _power_raw_to_w(ap)

    out: OutletRowData = [(None, None, None)] * OUTLETS_PER_PDU
    resp = cli.get_outlets_status()
    arr: Any = None
    if isinstance(resp, dict) and not resp.get("error"):
        arr = resp.get("data")
        if isinstance(arr, dict):
            for k in ("outlets", "list", "items"):
                v = arr.get(k)
                if isinstance(v, list):
                    arr = v
                    break
    if isinstance(arr, list):
        for item in arr:
            if not isinstance(item, dict):
                continue
            port = _slotno_to_port(item.get("slot_no"))
            if not port or port < 1 or port > OUTLETS_PER_PDU:
                continue
            st = item.get("state")
            if st is None:
                st = _dict_get_ci(item, "State")
            if st is None:
                st = item.get("status")
            state = _parse_state_to_bool(st)
            cur = _extract_float_ci(
                item,
                [
                    "current",
                    "outlet_current",
                    "curr",
                    "amp",
                    "amps",
                    "i",
                    "I",
                    "Current",
                ],
            )
            pwr_raw = _extract_float_ci(
                item,
                [
                    "power",
                    "active_power",
                    "outlet_power",
                    "watt",
                    "watts",
                    "p",
                    "P",
                    "Power",
                ],
            )
            pwr = _power_raw_to_w(pwr_raw)
            out[port - 1] = (cur, pwr, state)

    # JSON 不可达或无数/无状态时，用网页 CGI（与浏览器一致）补电流/功率/状态（否则 GUI 会一直“未在线”）
    force_html = (os.environ.get("PDU_HTML_METRICS") or "").strip().lower() in ("1", "true", "yes", "on")
    has_measure = any((c is not None) or (p is not None) for (c, p, _s) in out)
    has_state = any(s is not None for (_, _, s) in out)
    if force_html or (not has_measure) or (not has_state):
        try:
            from pdu_html_metrics import merge_outlet_rows_from_html

            u = (os.environ.get("PDU_HTML_USER") or "admin").strip()
            pw = (os.environ.get("PDU_HTML_PASSWORD") or "admin").strip()

            now = time.time()
            cached_out: Optional[OutletRowData] = None
            cached_total: Optional[float] = None
            with _HTML_METRICS_CACHE_LOCK:
                ent = _HTML_METRICS_CACHE.get(ip)
                if ent and (now - float(ent.get("ts", 0.0))) <= HTML_METRICS_CACHE_TTL_S:
                    cached_out = ent.get("out")
                    cached_total = ent.get("total")

            if cached_out is not None:
                # 只补 None，不覆盖已有值
                merged = list(out)
                for i in range(min(len(merged), len(cached_out))):
                    c0, p0, s0 = merged[i]
                    t1 = cached_out[i]
                    if isinstance(t1, (list, tuple)) and len(t1) >= 3:
                        c1, p1, s1 = t1[0], t1[1], t1[2]
                    elif isinstance(t1, (list, tuple)) and len(t1) == 2:
                        c1, p1, s1 = t1[0], t1[1], None
                    else:
                        continue
                    if c0 is None and c1 is not None:
                        c0 = c1
                    if p0 is None and p1 is not None:
                        p0 = p1
                    if s0 is None and s1 is not None:
                        s0 = s1
                    merged[i] = (c0, p0, s0)
                out = merged
                if (total_w is None or total_w <= 0) and isinstance(cached_total, (int, float)):
                    total_w = float(cached_total)
            else:
                out, total_w = merge_outlet_rows_from_html(ip, u, pw, out, total_w, OUTLETS_PER_PDU)
                with _HTML_METRICS_CACHE_LOCK:
                    _HTML_METRICS_CACHE[ip] = {"ts": now, "out": list(out), "total": total_w}
        except Exception:
            pass
    return out, total_w


class HttpFetchWorker(QThread):
    data_ready = Signal(object)

    def __init__(self, only_rows: Optional[List[int]] = None):
        super().__init__()
        self._only_rows = only_rows

    def run(self):
        res: Dict[int, Dict[str, Any]] = {}
        try:
            rows = [r for r in PDU_ONLINE_ROWS if 0 <= r < len(PDU_IPS)]
            if self._only_rows is not None:
                want = set(self._only_rows)
                rows = [r for r in rows if r in want]
            if not rows:
                self.data_ready.emit(res)
                return
            n_workers = max(1, min(len(rows), PDU_HTTP_FETCH_WORKERS))
            with ThreadPoolExecutor(max_workers=n_workers) as pool:
                fut_to_row: Dict[Any, int] = {}
                for r in rows:
                    fut = pool.submit(fetch_pdu_row_data_http, PDU_IPS[r])
                    fut_to_row[fut] = r
                for fut in as_completed(fut_to_row):
                    r = fut_to_row[fut]
                    try:
                        row_data, total_w = fut.result()
                        res[r] = {"data": row_data, "total_w": total_w}
                    except Exception:
                        pass
        except Exception:
            res = {}
        self.data_ready.emit(res)


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
        ok = PDUHttpClient(self._ip).set_outlet(self._outlet_index, self._on)
        self.set_done.emit(ok, self._row, self._col, self._on)


class AllOffWorker(QThread):
    finished_all = Signal()

    def __init__(self, rows: List[int], protected: Optional[set] = None):
        super().__init__()
        self._rows = rows
        self._protected = protected or set()

    def run(self):
        try:
            # 按 IP 汇总要断开的插座序号；同一 IP 串行下发，避免固件并发丢命令
            by_ip: Dict[str, List[int]] = {}
            for row in self._rows:
                if row < 0 or row >= len(PDU_IPS):
                    continue
                ip = PDU_IPS[row]
                for outlet_index in range(1, OUTLETS_PER_PDU + 1):
                    col = outlet_index - 1
                    if (row, col) in self._protected:
                        continue
                    by_ip.setdefault(ip, []).append(outlet_index)
            for _ip in list(by_ip.keys()):
                by_ip[_ip] = sorted(set(by_ip[_ip]))

            def _all_off_one_ip(ip: str, outlets: List[int]) -> None:
                cli = PDUHttpClient(ip)
                for oi in outlets:
                    ok = False
                    for attempt in range(1 + PDU_ALL_OFF_PER_OUTLET_RETRY):
                        if cli.set_outlet(oi, False):
                            ok = True
                            break
                        time.sleep(0.12)
                    if not ok:
                        pass
                    if PDU_ALL_OFF_STEP_S > 0:
                        time.sleep(PDU_ALL_OFF_STEP_S)

            if not by_ip:
                return
            n_par = min(PDU_ALL_OFF_IP_PARALLEL, len(by_ip))
            with ThreadPoolExecutor(max_workers=n_par) as pool:
                futs = [pool.submit(_all_off_one_ip, ip, outs) for ip, outs in by_ip.items()]
                for fut in as_completed(futs):
                    try:
                        fut.result()
                    except Exception:
                        pass
        finally:
            self.finished_all.emit()


class PduMonitorHttpWindow(QMainWindow):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("PDU 监控（HTTP API，带分组开机）— 6 台 × 6 插座")
        self.setMinimumSize(1300, 650)

        self._worker_busy = False
        self._worker: Optional[HttpFetchWorker] = None
        self._quick_fetch_workers: List[HttpFetchWorker] = []
        self._set_workers: List[OutletSetWorker] = []
        self._all_off_worker: Optional[AllOffWorker] = None
        self._all_off_in_progress = False
        self._online_rows_runtime: set[int] = set()
        self._manual_pending: Dict[Tuple[int, int], dict] = {}

        self._group_start_buttons: Dict[str, QPushButton] = {}
        self._group_set_workers: List[OutletSetWorker] = []
        self._group_start_busy = False
        self._group_ops_remaining = 0

        self._alias_config_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "pdu_aliases.json")
        self._pdu_data_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "pdu_data")
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        self._run_csv_path = os.path.join(self._pdu_data_dir, f"pdu_data_{ts}.csv")
        self._run_alias_path = os.path.join(self._pdu_data_dir, f"pdu_alias_{ts}.json")
        self._last_data_by_row: Dict[int, OutletRowData] = {}
        self._last_total_w_by_row: Dict[int, Optional[float]] = {}

        self._build_ui()
        self._load_aliases()

        self._poll_timer = QTimer(self)
        self._poll_timer.timeout.connect(self._refresh_all)
        self._poll_timer.start(PDU_POLL_MS)
        self._refresh_all()

        self._csv_timer = QTimer(self)
        self._csv_timer.timeout.connect(self._save_csv_auto)
        self._csv_timer.start(1000)

    def _update_total_power_header(self) -> None:
        """更新表头第 0 列：显示 6 台 PDU 总功率合计。"""
        try:
            n_rows = min(6, len(PDU_IPS))
            total_sum = 0.0
            has_any = False
            for r in range(n_rows):
                if r not in self._online_rows_runtime:
                    continue
                tw = self._last_total_w_by_row.get(r)
                if isinstance(tw, (int, float)):
                    total_sum += float(tw)
                    has_any = True
                else:
                    data = self._last_data_by_row.get(r)
                    if isinstance(data, list) and len(data) == OUTLETS_PER_PDU:
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
        top_btn_row.addWidget(btn_all_off, 1)
        group_layout.addLayout(top_btn_row)

        self._table = QTableWidget(6, 1 + OUTLETS_PER_PDU)
        self._table.setHorizontalHeaderLabels(["PDU / 总功率"] + [f"插座{i}" for i in range(1, OUTLETS_PER_PDU + 1)])
        self._table.horizontalHeader().setSectionResizeMode(QHeaderView.ResizeMode.Fixed)
        self._table.verticalHeader().setVisible(True)
        self._table.setAlternatingRowColors(True)
        self._table.setStyleSheet(
            "QTableWidget { font-size: 12px; gridline-color: #ddd; }"
            "QHeaderView::section { background: #e8e8e8; padding: 6px; font-weight: bold; }"
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
        self._cell_outlet_alias: List[List[QLineEdit]] = []
        self._cell_protected: List[List[QCheckBox]] = []
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

                alias_edit = QLineEdit()
                alias_edit.setPlaceholderText("别名")
                alias_edit.setMaxLength(32)
                alias_edit.setStyleSheet("font-size: 11px;")
                alias_edit.editingFinished.connect(self._save_aliases)
                self._cell_outlet_alias[row].append(alias_edit)

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

                top = QHBoxLayout()
                status_indicator = QFrame()
                status_indicator.setFixedSize(STATUS_INDICATOR_W, STATUS_INDICATOR_H)
                status_indicator.setStyleSheet("background-color: #c00; border: 1px solid #888; border-radius: 2px;")
                self._cell_status_labels[row].append(status_indicator)
                top.addWidget(status_indicator)

                btn_on = QPushButton("开")
                btn_on.setFixedWidth(32)
                f = btn_on.font()
                f.setBold(True)
                btn_on.setFont(f)
                btn_on.clicked.connect(lambda _=False, r=row, c=col: self._on_outlet_control(r, c, True))
                self._cell_btn_on[row].append(btn_on)
                top.addWidget(btn_on)

                btn_off = QPushButton("关")
                btn_off.setFixedWidth(32)
                f2 = btn_off.font()
                f2.setBold(True)
                btn_off.setFont(f2)
                btn_off.clicked.connect(lambda _=False, r=row, c=col: self._on_outlet_control(r, c, False))
                self._cell_btn_off[row].append(btn_off)
                top.addWidget(btn_off)

                cmb_group = QComboBox()
                cmb_group.setFixedWidth(60)
                cmb_group.addItem("")
                for ch in "ABCDEFGHIJ":
                    cmb_group.addItem(ch)
                cmb_index = QComboBox()
                cmb_index.setFixedWidth(55)
                cmb_index.addItem("")
                for i in range(10):
                    cmb_index.addItem(str(i))
                cmb_group.currentIndexChanged.connect(self._on_group_config_changed)
                cmb_index.currentIndexChanged.connect(self._on_group_config_changed)
                self._cell_group_letter[row].append(cmb_group)
                self._cell_group_index[row].append(cmb_index)
                top.addWidget(cmb_group)
                top.addWidget(cmb_index)
                top.addStretch()
                lay.addLayout(top)

                cur_lbl = QLabel("电流 -A")
                pwr_lbl = QLabel("功率 -W")
                self._cell_current_labels[row].append(cur_lbl)
                self._cell_power_labels[row].append(pwr_lbl)
                lay.addWidget(cur_lbl)
                lay.addWidget(pwr_lbl)

                self._table.setCellWidget(row, col + 1, frame)

        group_layout.addWidget(self._table)

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
            le.editingFinished.connect(self._save_aliases)
            self._group_alias_edits[ch] = le
            cell_layout.addWidget(le)
            group_row.addWidget(cell)
        group_row.addStretch()
        group_layout.addLayout(group_row)

        layout.addWidget(group)

        self._status = QStatusBar()
        self.setStatusBar(self._status)

    # ---- alias/group/protected save/load ----
    def _load_aliases(self):
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
                if is_prot:
                    self._outlet_states[row][col] = True

        self._refresh_group_buttons_enabled()

    def _save_aliases(self):
        outlet_alias = {}
        for row in range(len(self._cell_outlet_alias)):
            for col in range(len(self._cell_outlet_alias[row])):
                text = self._cell_outlet_alias[row][col].text().strip()
                if text:
                    outlet_alias[f"{row}_{col}"] = text
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
        group_alias = {}
        for ch in "ABCDEFGHIJ":
            text = self._group_alias_edits[ch].text().strip()
            if text:
                group_alias[ch] = text

        payload = {
            "outlet_alias": outlet_alias,
            "outlet_group": outlet_group,
            "outlet_protected": outlet_protected,
            "group_alias": group_alias,
            "updated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        }
        try:
            with open(self._alias_config_path, "w", encoding="utf-8") as f:
                json.dump(payload, f, ensure_ascii=False, indent=2)
        except Exception:
            pass
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

    # ---- group ----
    def _collect_groups(self) -> Dict[str, List[Tuple[int, int, int]]]:
        groups: Dict[str, List[Tuple[int, int, int]]] = {}
        for row in range(6):
            for col in range(OUTLETS_PER_PDU):
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
        groups = self._collect_groups()
        busy = self._group_start_busy
        for ch, btn in self._group_start_buttons.items():
            btn.setEnabled((ch in groups and len(groups[ch]) > 0) and not busy)

    def _on_group_config_changed(self, *_):
        self._refresh_group_buttons_enabled()
        self._save_aliases()

    def _group_op_finished(self) -> None:
        if self._group_ops_remaining > 0:
            self._group_ops_remaining -= 1
            if self._group_ops_remaining <= 0:
                self._group_ops_remaining = 0
                self._group_start_busy = False
        self._refresh_group_buttons_enabled()

    def _group_on_outlet_control(self, row: int, col: int, on: bool):
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
            if success:
                self._status.showMessage(f"[组开机] Pdu{r + 1} 插座{c + 1} 写入成功，等待确认…")
                self._refresh_all()
            else:
                self._status.showMessage(f"[组开机] Pdu{r + 1} 插座{c + 1} 控制失败")
            worker.deleteLater()
            self._group_op_finished()

        worker.set_done.connect(_on_done, Qt.ConnectionType.QueuedConnection)
        worker.start()

    def _on_group_start(self, group_letter: str):
        if self._group_start_busy:
            self._status.showMessage("组开机序列进行中，请等待当前批次完成后再操作")
            return
        groups = self._collect_groups()
        if group_letter not in groups or not groups[group_letter]:
            self._status.showMessage(f"{group_letter} 组当前没有配置插座")
            return
        items = groups[group_letter]
        need_close: List[Tuple[int, int, int]] = []
        for order, row, col in items:
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
            QTimer.singleShot(delay, lambda r=row, c=col: self._group_on_outlet_control(r, c, True))
        names = ", ".join(f"Pdu{r + 1}-插座{c + 1}#{order}" for order, r, c in need_close)
        self._status.showMessage(
            f"已启动 {group_letter} 组开机（仅对当前断开的插座，间隔 {interval_ms / 1000:g} 秒）：{names}"
        )

    # ---- CSV ----
    def _get_csv_headers(self) -> List[str]:
        headers = ["date_time"]
        for pdu_num in range(1, 7):
            headers.append(f"pdu{pdu_num}_total_power")
            for out in range(1, OUTLETS_PER_PDU + 1):
                headers.append(f"pdu{pdu_num}_outlet{out}_current")
                headers.append(f"pdu{pdu_num}_outlet{out}_power")
        return headers

    def _append_csv_row(self) -> Optional[str]:
        os.makedirs(self._pdu_data_dir, exist_ok=True)
        headers = self._get_csv_headers()
        row_values = [datetime.now().strftime("%Y-%m-%d %H:%M:%S")]
        for row in range(6):
            data = self._last_data_by_row.get(row, [(None, None, None)] * OUTLETS_PER_PDU)
            total = self._last_total_w_by_row.get(row)
            if total is None:
                total = sum((p or 0) for _, p, _ in data)
            row_values.append(str(int(total)) if total else "")
            for col in range(OUTLETS_PER_PDU):
                cur, pwr = (data[col][0], data[col][1]) if col < len(data) else (None, None)
                row_values.append(f"{cur:.3f}" if cur is not None else "")
                row_values.append(f"{pwr:.1f}" if pwr is not None else "")
        try:
            write_header = not os.path.isfile(self._run_csv_path) or os.path.getsize(self._run_csv_path) == 0
            with open(self._run_csv_path, "a", newline="", encoding="utf-8-sig") as f:
                w = csv.writer(f)
                if write_header:
                    w.writerow(headers)
                w.writerow(row_values)
            return self._run_csv_path
        except Exception:
            return None

    def _save_csv_auto(self):
        self._append_csv_row()

    def _save_data_to_files(self):
        csv_path = self._append_csv_row()
        if csv_path is None:
            self._status.showMessage("保存 CSV 失败")
            return
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        alias_path = os.path.join(self._pdu_data_dir, f"pdu_alias_{ts}.json")
        try:
            with open(alias_path, "w", encoding="utf-8") as f:
                json.dump({"saved_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S")}, f, ensure_ascii=False, indent=2)
        except Exception as e:
            self._status.showMessage(f"保存别名 JSON 失败: {e}")
            return
        self._status.showMessage(f"已保存: {os.path.basename(csv_path)} 与 {os.path.basename(alias_path)}")
        QMessageBox.information(self, "保存完成", f"已保存到 pdu_data 目录：\n{os.path.basename(csv_path)}\n{os.path.basename(alias_path)}")

    # ---- top banner & apply ----
    def _update_top_info_banner(self) -> None:
        n = len(PDU_IPS)
        online_sorted = sorted(self._online_rows_runtime)
        if online_sorted:
            online_parts = [f"Pdu{r + 1}（{PDU_IPS[r]}）" for r in online_sorted if 0 <= r < n]
            online_str = "，".join(online_parts)
        else:
            online_str = "无（请检查网络与HTTP API可达性）"
        self._info_label.setText(f"PDU 监控（HTTP） | 当前在线: {online_str}")

    def _apply_row_data(
        self,
        row: int,
        data: OutletRowData,
        total_str: str,
        is_online: Optional[bool] = None,
        total_w: Optional[float] = None,
    ):
        if is_online is None:
            is_online = row in self._online_rows_runtime
        ip = PDU_IPS[row]
        status = "在线" if is_online else "未在线"
        pdu_label = f"Pdu{row + 1} {status}\n{ip}"
        item0 = QTableWidgetItem(f"{pdu_label}\n功率 {total_str}")
        if not is_online:
            item0.setBackground(QBrush(QColor("#e9ecef")))
        self._table.setItem(row, 0, item0)
        self._last_data_by_row[row] = list(data)
        self._last_total_w_by_row[row] = total_w
        for col, item in enumerate(data):
            cur, pwr, state = item
            cur_text = f"{cur:.3f}A" if cur is not None else "-A"
            # 未返回电流/功率时显示 “-”，勿用 0 冒充实测值
            pwr_text = f"{pwr:.1f}W" if pwr is not None else "-W"
            self._cell_current_labels[row][col].setText(f"电流 {cur_text}")
            self._cell_power_labels[row][col].setText(f"功率 {pwr_text}")
            want = "#888" if not is_online else ("#0a0" if state is True else "#c00")
            self._cell_status_labels[row][col].setStyleSheet(
                f"background-color: {want}; border: 1px solid #666; border-radius: 2px;"
            )
            pend = self._manual_pending.get((row, col))
            if state is not None:
                self._outlet_states[row][col] = state
                if pend is not None and state is pend.get("desired"):
                    self._manual_pending.pop((row, col), None)
            # 按钮
            btn_on = self._cell_btn_on[row][col]
            btn_off = self._cell_btn_off[row][col]
            pend = self._manual_pending.get((row, col))
            if pend is not None:
                started_at = float(pend.get("started_at") or 0.0)
                if started_at > 0 and (time.time() - started_at) > MANUAL_CONFIRM_TIMEOUT_S:
                    self._manual_pending.pop((row, col), None)
                else:
                    btn_on.setEnabled(False)
                    btn_off.setEnabled(False)
                    continue
            protected = self._cell_protected[row][col].isChecked()
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
        self._refresh_group_buttons_enabled()

    @Slot()
    def _on_worker_finished(self):
        self._worker_busy = False
        w = self._worker
        self._worker = None
        if w is not None:
            w.deleteLater()

    @Slot(object)
    def _on_pdu_data(self, data_by_row: object):
        if self._all_off_in_progress:
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
                self._apply_row_data(row, dead, "-", is_online=False, total_w=None)
            self._update_top_info_banner()
            self._update_total_power_header()
            return
        for row, payload in data_by_row.items():
            if row >= n_rows or not isinstance(payload, dict):
                continue
            data = payload.get("data")
            total_w = payload.get("total_w")
            if not isinstance(data, list) or len(data) != OUTLETS_PER_PDU:
                continue
            has_valid = any((c is not None) or (p is not None) or (s is not None) for c, p, s in data)
            if has_valid:
                self._online_rows_runtime.add(row)
            else:
                self._online_rows_runtime.discard(row)
            if isinstance(total_w, (int, float)) and total_w >= 0:
                total_str = f"{int(round(float(total_w)))}W" if has_valid else "-"
                total_w2: Optional[float] = float(total_w)
            else:
                total_power = sum((p or 0) for _, p, _ in data)
                total_str = f"{int(round(total_power))}W" if has_valid else "-"
                total_w2 = total_power if has_valid else None
            self._apply_row_data(row, data, total_str, is_online=has_valid, total_w=total_w2)
        self._update_top_info_banner()
        self._update_total_power_header()

    def _refresh_all(self):
        for row in range(len(PDU_IPS)):
            if row not in PDU_ONLINE_ROWS:
                dead = [(None, None, None)] * OUTLETS_PER_PDU
                self._apply_row_data(row, dead, "-", is_online=False, total_w=None)
        if not self._worker_busy:
            for row in PDU_ONLINE_ROWS:
                if row >= len(PDU_IPS) or row in self._online_rows_runtime:
                    continue
                dead = [(None, None, None)] * OUTLETS_PER_PDU
                self._apply_row_data(row, dead, "-", is_online=False, total_w=None)
        if not self._worker_busy and PDU_ONLINE_ROWS and not self._all_off_in_progress:
            self._worker_busy = True
            self._worker = HttpFetchWorker()
            self._worker.data_ready.connect(self._on_pdu_data, Qt.ConnectionType.QueuedConnection)
            self._worker.finished.connect(self._on_worker_finished, Qt.ConnectionType.QueuedConnection)
            self._worker.start()
        else:
            self._update_top_info_banner()

    def _start_priority_fetch(self, rows: List[int]) -> None:
        """单行/少行刷新：不占用 _worker_busy，用于开关后立即拉状态（避免排队等整轮轮询）。"""
        clean = [r for r in rows if 0 <= r < len(PDU_IPS)]
        if not clean:
            return
        for r in clean:
            with _HTML_METRICS_CACHE_LOCK:
                _HTML_METRICS_CACHE.pop(PDU_IPS[r], None)
        qw = HttpFetchWorker(only_rows=clean)
        self._quick_fetch_workers.append(qw)
        qw.data_ready.connect(self._on_pdu_data, Qt.ConnectionType.QueuedConnection)

        def _qw_done() -> None:
            try:
                self._quick_fetch_workers.remove(qw)
            except ValueError:
                pass
            qw.deleteLater()

        qw.finished.connect(_qw_done, Qt.ConnectionType.QueuedConnection)
        qw.start()

    def _on_outlet_control(self, row: int, col: int, on: bool):
        if not on and self._cell_protected[row][col].isChecked():
            self._status.showMessage(f"Pdu{row + 1} 插座{col + 1} 为关键供电口：禁止断开")
            return
        if row not in self._online_rows_runtime:
            self._status.showMessage(f"Pdu{row + 1} 未在线，无法控制")
            return
        outlet_index = col + 1
        self._manual_pending[(row, col)] = {"desired": bool(on), "started_at": time.time(), "prev": self._outlet_states[row][col]}
        self._cell_btn_on[row][col].setEnabled(False)
        self._cell_btn_off[row][col].setEnabled(False)
        self._status.showMessage(f"正在下发 Pdu{row + 1} 插座{outlet_index} {'闭合' if on else '断开'}…")

        ip = PDU_IPS[row]
        worker = OutletSetWorker(ip, outlet_index, on, row, col)
        self._set_workers.append(worker)

        def _on_done(success: bool, r: int, c: int, o: bool):
            try:
                self._set_workers.remove(worker)
            except ValueError:
                pass
            if success:
                self._status.showMessage(f"Pdu{r + 1} 插座{c + 1} 写入成功，正在确认状态…")
                self._start_priority_fetch([r])
            else:
                # 失败回滚按钮（颜色由轮询决定）
                prev = self._manual_pending.get((r, c), {}).get("prev", False)
                self._manual_pending.pop((r, c), None)
                self._outlet_states[r][c] = bool(prev)
                self._status.showMessage(f"Pdu{r + 1} 插座{c + 1} 控制失败")
            worker.deleteLater()

        worker.set_done.connect(_on_done, Qt.ConnectionType.QueuedConnection)
        worker.start()

    def _on_all_off_clicked(self):
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
                if self._cell_protected[r][c].isChecked():
                    protected.add((r, c))
        for r in online_rows:
            for c in range(OUTLETS_PER_PDU):
                if (r, c) in protected:
                    continue
                self._cell_btn_on[r][c].setEnabled(False)
                self._cell_btn_off[r][c].setEnabled(False)
        self._all_off_worker = AllOffWorker(online_rows, protected=protected)
        self._all_off_worker.finished_all.connect(self._on_all_off_done)
        self._all_off_worker.start()
        self._status.showMessage(f"正在断开全部插座…（已跳过关键口 {len(protected)} 个）" if protected else "正在断开全部插座…")

    def _on_all_off_done(self):
        if self._all_off_worker:
            self._all_off_worker.deleteLater()
            self._all_off_worker = None
        self._all_off_in_progress = False
        self._refresh_all()
        self._status.showMessage("全部插座已断开")

    def closeEvent(self, event: QCloseEvent) -> None:
        self._poll_timer.stop()
        self._csv_timer.stop()
        self._worker_busy = False
        wk = self._worker
        self._worker = None
        if wk is not None:
            if wk.isRunning():
                wk.wait(1200)
                if wk.isRunning():
                    wk.terminate()
                    wk.wait(300)
            wk.deleteLater()
        for qw in list(self._quick_fetch_workers):
            if qw.isRunning():
                qw.wait(1200)
                if qw.isRunning():
                    qw.terminate()
                    qw.wait(300)
            try:
                self._quick_fetch_workers.remove(qw)
            except ValueError:
                pass
            qw.deleteLater()
        self._quick_fetch_workers.clear()
        for w in list(self._set_workers):
            if w.isRunning():
                w.wait(1200)
                if w.isRunning():
                    w.terminate()
                    w.wait(300)
            w.deleteLater()
        self._set_workers.clear()
        for w in list(self._group_set_workers):
            if w.isRunning():
                w.wait(1200)
                if w.isRunning():
                    w.terminate()
                    w.wait(300)
            w.deleteLater()
        self._group_set_workers.clear()
        aw = self._all_off_worker
        self._all_off_worker = None
        if aw is not None:
            if aw.isRunning():
                aw.wait(3000)
                if aw.isRunning():
                    aw.terminate()
                    aw.wait(300)
            aw.deleteLater()
        event.accept()


def main():
    app = QApplication([])
    app.setStyle("Fusion")
    win = PduMonitorHttpWindow()
    win.show()
    app.exec()


if __name__ == "__main__":
    main()

