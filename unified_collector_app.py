# -*- coding: utf-8 -*-
"""
多仪器统一采集主程序 — 扫描 config、树形列表、串口连接；选中设备显示绘图变量与实时曲线。

全部仪器设备数据保存为 CSV，规则如下：
- 保存目录：.\\data\\yyyymmdd_hhmmss\\（会话启动时创建）
- 通用命名：仪器设备名_yyyymmdd_hhmmss.csv，仪器名由 JSON 的 device_id/name 或 config 文件名决定
- 设备与文件：
  GPS/NTP、YGDS、Grimm(32ch)、PCASP、CVI/ISO、CPC3788 等 → _append_device_csv → 仪器设备名_timestamp.csv
  Grimm P 行（PM2.5/PM10 等）→ _append_grimm_P_csv → 仪器设备名_P_timestamp.csv
  NOx-O3 → _append_nox_temp_csv → 仪器设备名_timestamp.csv（39 列，与附带 APP Temp 格式一致）
NOx：仅做设备上传数据的接收、显示与记录，不包含附带 APP 的阀门/反应炉等控制功能。
"""
import csv
import json
import math
import os
import re
import struct
import sys
import time
from collections import deque
from datetime import datetime, timezone
from typing import Optional, Dict, Any, List, Tuple

import serial
import serial.tools.list_ports
from PySide6.QtWidgets import (
    QApplication,
    QMainWindow,
    QWidget,
    QVBoxLayout,
    QHBoxLayout,
    QGridLayout,
    QTableWidget,
    QTableWidgetItem,
    QPushButton,
    QComboBox,
    QLabel,
    QGroupBox,
    QSplitter,
    QStatusBar,
    QMessageBox,
    QHeaderView,
    QScrollArea,
    QCheckBox,
    QFrame,
    QFormLayout,
    QTabWidget,
    QPlainTextEdit,
)
from PySide6.QtCore import Qt, QTimer
from PySide6.QtGui import QFont

import matplotlib
matplotlib.use("QtAgg")
import matplotlib.dates as mdates
from matplotlib.backends.backend_qtagg import FigureCanvasQTAgg as FigureCanvas
from matplotlib.figure import Figure

# 复用 PCASP 协议/解析/换算（与 pcasp_receiver.py 一致）
from pcasp_receiver import (
    build_get_data_cmd as _pcasp_build_get_data_cmd,
    build_init_cmd as _pcasp_build_init_cmd,
    parse_frame as _pcasp_parse_frame,
    ad_to_physical as _pcasp_ad_to_physical,
    read_exact as _pcasp_read_exact,
)

# 项目根目录（脚本所在目录）
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
if SCRIPT_DIR not in sys.path:
    sys.path.insert(0, SCRIPT_DIR)
DEFAULT_DEVICES_JSON = os.path.join(SCRIPT_DIR, "config", "devices.json")

# NOx-O3 Temp.csv 与附带 APP 记录格式一致（如 data/20260311135052Temp.csv）：表头「日期 时间」+ 39 列中文
NOX_TEMP_CSV_HEADER = (
    "日期 时间",
    "NO2转化炉", "O3发生", "NO预反应阀", "NOx预反应阀", "NOx设备状态", "NOx测量状态",
    "NO样气流量", "NOx样气流量", "NO O3流量", "NOx O3流量", "NO 压力", "NOx 压力",
    "转化炉温度", "转化炉电流", "NO 反应室温度", "NOx 反应室温度",
    "NO 探测器温度", "NOx 探测器温度", "NO 探测器值", "NOx 探测器值",
    "NO 值", "NOx 值", "NO 1Min值", "NOx 1Min值", "NO  初始状态", "NOx 初始状态",
    "NO发生", "O3设备状态", "O3测量状态", "O3样气流量", "O3 NO流量", "O3压力",
    "O3反应室温度", "O3探测器温度", "O3探测器", "O3值", "O3 1Min值", "O3  初始状态",
)
NOX_TEMP_CSV_FIELDS = (
    "converter_status", "o3_gen_status", "no_pre_react_status", "nox_pre_react_status",
    "nox_device_status", "nox_measure_status", "no_sample_flow", "nox_sample_flow",
    "no_o3_flow", "nox_o3_flow", "no_pressure", "nox_pressure",
    "converter_temp", "converter_current", "no_reactor_temp", "nox_reactor_temp",
    "no_detector_temp", "nox_detector_temp", "no_detector_value", "nox_detector_value",
    "no_conc", "nox_conc", "no_conc_1min_avg", "nox_conc_1min_avg",
    "no_init_status", "nox_init_status", "o3_no_gen_status", "o3_device_status", "o3_measure_status",
    "o3_sample_flow", "o3_no_flow", "o3_pressure", "o3_reactor_temp", "o3_detector_temp",
    "o3_detector_value", "o3_conc", "o3_conc_1min_avg", "o3_init_status",
)

# NOx-O3 协议：连接后需发送「启动数采数据传输」设备才会上传数据（见 nox_o3_collector）
try:
    from nox_o3_collector import NOxO3Parser, Config as NOxConfig, ParamID
    _NOX_PROTOCOL_AVAILABLE = True
except ImportError:
    NOxO3Parser = None
    NOxConfig = None
    ParamID = None
    _NOX_PROTOCOL_AVAILABLE = False
# ISO/CVI：向等速采样头发送 TAS/温度（airspext/oatmpext），需先发配置命令使设备接收外部数据
try:
    from iso_cvi_protocol import encode_tas_temperature_iso_cvi
    _ISO_CVI_PROTOCOL_AVAILABLE = True
except ImportError:
    encode_tas_temperature_iso_cvi = None
    _ISO_CVI_PROTOCOL_AVAILABLE = False
DEFAULT_BAUDRATES = ["115200", "38400", "57600", "9600"]
ISO_CVI_SEND_INTERVAL = 0.5  # 向 CVI/ISO 发送 TAS/温度的间隔（秒）
MAX_PLOT_POINTS = 300
PLOT_POLL_MS = 1000   # 串口轮询与采集 1 Hz
SIM_TICK_MS = 1000    # 模拟数据/保存/显示 1 Hz（Grimm 除外：6s 一次，收到即保存）
NUM_CHANNELS_GRIMM = 32  # GRIMM：仅打开串口、被动等待 COM 口数据（设备每 6s 发送一次），不发送任何命令

# CPC3788 命令与字段映射（命令 -> 返回单值）
_CPC3788_CMD_MAP = {
    "concentration": "RD",
    "inlet_flow": "RIF",
    "sample_flow": "RSF",
    "cabinet_temp": "RTA",
    "conditioner_temp": "RTC",
    "growth_tube_temp": "RTG",
    "optics_temp": "RTO",
    "absolute_pressure": "RPA",
    "laser_current": "RLP",
    "liquid_level": "RLL",
}


def _cpc3788_send_command(ser, cmd: str) -> Optional[str]:
    if not ser or not ser.is_open:
        return None
    if not cmd.endswith("\r"):
        cmd = cmd + "\r"
    try:
        ser.reset_input_buffer()
        ser.write(cmd.encode("ascii"))
        time.sleep(0.15)
        resp = ser.read_all().decode("ascii", errors="ignore").strip()
        return resp if resp and resp != "ERROR" else None
    except Exception:
        return None


def read_gps_timea(ser) -> Optional[Dict[str, Any]]:
    """从 GPS 串口读一行 #TIMEA 并解析为 last_data 格式（year,month,day,hour,minute,second_ms,second,ms）。无数据返回 None。"""
    try:
        if ser.in_waiting == 0:
            return None
        old_timeout = ser.timeout
        try:
            ser.timeout = 0.05
            line = ser.readline().decode("ascii", errors="ignore").strip()
        finally:
            ser.timeout = old_timeout
        if not line.startswith("#TIMEA"):
            return None
        semicolon_pos = line.find(";")
        if semicolon_pos == -1:
            return None
        data_part = line[semicolon_pos + 1:]
        valid_pos = data_part.rfind(",VALID")
        if valid_pos == -1:
            return None
        time_fields = data_part[:valid_pos].split(",")
        if len(time_fields) < 6:
            return None
        year = int(time_fields[-6])
        month = int(time_fields[-5])
        day = int(time_fields[-4])
        hour = int(time_fields[-3])
        minute = int(time_fields[-2])
        second_ms = int(time_fields[-1])
        seconds = second_ms // 1000
        ms = second_ms % 1000
        return {
            "year": year, "month": month, "day": day,
            "hour": hour, "minute": minute,
            "second_ms": second_ms, "second": seconds, "ms": ms,
        }
    except Exception:
        return None


YGDS_FRAME_SIZE = 103
YGDS_HEAD = (0xAA, 0x55)
YGDS_TAIL = (0xAB, 0xAB)


def read_ygds_frame(ser, buffer: bytearray) -> Tuple[Optional[Dict[str, Any]], bytearray]:
    """从串口读入字节，在 buffer 中找完整 YGDS 帧，解析返回 (last_data 字典 或 None, 更新后的 buffer)。"""
    try:
        if ser.in_waiting > 0:
            buffer.extend(ser.read(ser.in_waiting))
    except Exception:
        return None, buffer
    while len(buffer) >= YGDS_FRAME_SIZE:
        start = -1
        for i in range(len(buffer) - 1):
            if buffer[i] == YGDS_HEAD[0] and buffer[i + 1] == YGDS_HEAD[1]:
                start = i
                break
        if start < 0:
            del buffer[:]
            break
        if start + YGDS_FRAME_SIZE > len(buffer):
            del buffer[:start]
            break
        if buffer[start + 101] != YGDS_TAIL[0] or buffer[start + 102] != YGDS_TAIL[1]:
            buffer.pop(0)
            continue
        data = bytes(buffer[start : start + YGDS_FRAME_SIZE])
        del buffer[: start + YGDS_FRAME_SIZE]
        calc_checksum = sum(data[2:100]) & 0xFF
        if calc_checksum != data[100]:
            continue
        parsed = {
            "BlockHead0": data[0], "BlockHead1": data[1],
            "DataValidity": list(data[2:7]),
            "Ps": struct.unpack("<H", data[8:10])[0] / 16.0,
            "Qc": struct.unpack("<H", data[10:12])[0] / 128.0,
            "AOA": struct.unpack("<h", data[12:14])[0] / 256.0,
            "AOS": struct.unpack("<h", data[14:16])[0] / 256.0,
            "Hp": struct.unpack("<h", data[16:18])[0] / 2.0,
            "Ma": struct.unpack("<H", data[18:20])[0] / 32768.0,
            "Vi": struct.unpack("<H", data[20:22])[0] / 64.0,
            "Tt": struct.unpack("<h", data[22:24])[0] / 128.0,
            "Ts": struct.unpack("<h", data[24:26])[0] / 128.0,
            "Vt": struct.unpack("<H", data[26:28])[0] / 64.0,
            "Lwc": struct.unpack("<H", data[28:30])[0] / 256.0,
            "WDSN": struct.unpack("<h", data[30:32])[0] / 128.0,
            "WDSE": struct.unpack("<h", data[32:34])[0] / 128.0,
            "WDS": struct.unpack("<H", data[34:36])[0] / 128.0,
            "WDD": struct.unpack("<H", data[36:38])[0] / 64.0,
            "WDSV": struct.unpack("<h", data[38:40])[0] / 128.0,
            "GPSAlt": struct.unpack("<h", data[40:42])[0] / 2.0,
            "VELN": struct.unpack("<h", data[42:44])[0] / 32.0,
            "VELE": struct.unpack("<h", data[44:46])[0] / 32.0,
            "VELD": struct.unpack("<h", data[46:48])[0] / 32.0,
            "FaultA": struct.unpack("<H", data[82:84])[0],
            "FaultB": struct.unpack("<H", data[48:50])[0],
            "FaultC": struct.unpack("<H", data[50:52])[0],
            "ROLL": struct.unpack("<h", data[52:54])[0] / 128.0,
            "PITCH": struct.unpack("<h", data[54:56])[0] / 128.0,
            "YAW": struct.unpack("<H", data[56:58])[0] / 64.0,
            "Psu": struct.unpack("<H", data[58:60])[0] * 4,
            "Qcu": struct.unpack("<H", data[60:62])[0] / 2.0,
            "dPaoa": struct.unpack("<h", data[62:64])[0] / 2.0,
            "dPaos": struct.unpack("<h", data[64:66])[0] / 2.0,
            "Hum": struct.unpack("<H", data[68:70])[0] / 256.0,
            "CUR": struct.unpack("<H", data[72:74])[0] / 256.0,
            "SLWC": struct.unpack("<H", data[74:76])[0] * 2,
            "SoftwareVersion": data[80] / 100.0,
            "WorkCounter": data[81],
            "longitude": struct.unpack("<d", data[84:92])[0],
            "latitude": struct.unpack("<d", data[92:100])[0],
            "CheckSum": data[100],
        }
        return parsed, buffer
    return None, buffer


def _grimm_extract_numbers(line: str) -> List[float]:
    """从 GRIMM 数据行中提取数字（通道计数值等）。"""
    numbers = re.findall(r"-?\d+\.?\d*", line)
    return [float(n) for n in numbers]


# GRIMM 串口数据：保留大写 C 与小写 c 开头的行（4 行×8 数=32 通道）；P、J 等开头舍去。
# 格式示例：C1: 6120 3880... / C1; 65 45... / c1: 7 3... / c1; 0 0...  每行 8 个数，共 4 行 32 通道。
GRIMM_LINES_PER_FRAME = 4
GRIMM_NUMBERS_PER_LINE = 8


def _grimm_is_concentration_line(line: str) -> bool:
    """仅保留 C 或 c 开头的行（P、J 等舍去），用于拼成 32 通道。格式：C/c+数字+:;/; 如 C1:, C1;, c1:, c1;。"""
    line = line.strip()
    if not line or len(line) < 2:
        return False
    if line[0].upper() != "C":
        return False
    if len(line) >= 3 and line[2] in (":", ";"):
        return line[1].isdigit() or line[1] == "_"
    if len(line) >= 2 and line[1] in (":", ";"):
        return True
    return False


def _grimm_cycle_index(line: str) -> Optional[int]:
    """从 C/c 行取周期号（C8: -> 8, c9; -> 9）。C_:/c_; 返回 None，用于跨周期不拼帧。"""
    line = line.strip()
    if not line or len(line) < 2:
        return None
    if line[0].upper() != "C":
        return None
    if len(line) >= 3 and line[2] in (":", ";"):
        if line[1].isdigit():
            return int(line[1])
        return None
    if len(line) >= 2 and line[1] in (":", ";"):
        return None
    return None


def _grimm_is_P_line(line: str) -> bool:
    """P 开头的行：PM2.5/PM10 等相关量，需单独保存到 grimm_P_yyyymmdd_hhmmss.csv。格式：P 后接制表符/空格再跟数字列。"""
    line = line.strip()
    if not line or line[0].upper() != "P":
        return False
    if len(line) == 1:
        return True
    return line[1:2] in ("\t", " ") or (line[1:2].isdigit()) or (line[1:2] == "_")


def _grimm_parse_P_line(line: str) -> List[Any]:
    """解析 P 行：按空白分割，首列为 P，其余为数值（_14 视为 14）。返回 [P1, P2, ...] 数值列表。"""
    parts = line.strip().split()
    if not parts or parts[0].upper() != "P":
        return []
    values = []
    for p in parts[1:]:
        s = p.strip().lstrip("_")
        if not s:
            values.append("")
            continue
        try:
            values.append(float(s))
        except ValueError:
            values.append(p)
    return values


def read_grimm_frame(ser, row: Dict[str, Any], p_lines_collector: Optional[List[List[Any]]] = None) -> Optional[Tuple[Dict[str, Any], str]]:
    """GRIMM：收齐 4 行 C/c 浓度行（每行至少 8 个数）拼成 32 通道。与 grimm_simple 一致：有数据就 readline，短超时，不完整行缓冲。"""
    try:
        line_buffer = row.get("_grimm_line_buffer")
        if line_buffer is None:
            line_buffer = []
            row["_grimm_line_buffer"] = line_buffer
        partial = row.get("_grimm_partial", "")
        old_timeout = ser.timeout
        try:
            ser.timeout = 0.03
            for _ in range(20):
                if ser.in_waiting == 0 and not partial:
                    break
                raw = ser.readline()
                decoded = raw.decode("ascii", errors="ignore")
                line = (partial + decoded).strip()
                if not decoded.endswith("\n") and not decoded.endswith("\r"):
                    row["_grimm_partial"] = partial + decoded
                    partial = ""
                    break
                row["_grimm_partial"] = ""
                partial = ""
                if not line:
                    continue
                if p_lines_collector is not None and _grimm_is_P_line(line):
                    vals = _grimm_parse_P_line(line)
                    if vals:
                        p_lines_collector.append(vals)
                    continue
                if not _grimm_is_concentration_line(line):
                    continue
                nums = _grimm_extract_numbers(line)
                if len(nums) < GRIMM_NUMBERS_PER_LINE:
                    continue
                cycle = _grimm_cycle_index(line)
                if cycle is None:
                    continue
                if line_buffer:
                    first_cycle = _grimm_cycle_index(line_buffer[0])
                    if first_cycle is not None and first_cycle != cycle:
                        line_buffer.clear()
                line_buffer.append(line)
                if len(line_buffer) > GRIMM_LINES_PER_FRAME:
                    line_buffer.pop(0)
                if len(line_buffer) == GRIMM_LINES_PER_FRAME:
                    four = list(line_buffer)
                    cycles = [_grimm_cycle_index(ln) for ln in four]
                    if None in cycles or len(set(cycles)) != 1:
                        line_buffer.clear()
                        continue
                    all_nums = []
                    for ln in four:
                        n = _grimm_extract_numbers(ln)
                        # 每行可能含 C1/c1 的数码，只取最后 8 个为通道值
                        all_nums.extend(n[-GRIMM_NUMBERS_PER_LINE:] if len(n) >= GRIMM_NUMBERS_PER_LINE else n)
                    line_buffer.clear()
                    if len(all_nums) >= NUM_CHANNELS_GRIMM:
                        out = {f"Ch{i+1}": 0.0 for i in range(NUM_CHANNELS_GRIMM)}
                        for i in range(NUM_CHANNELS_GRIMM):
                            out[f"Ch{i+1}"] = all_nums[i] if i < len(all_nums) else 0.0
                        return out, "\n".join(four)
        finally:
            ser.timeout = old_timeout
        return None
    except Exception:
        return None


def read_cpc3788_sample(ser) -> Dict[str, Any]:
    """从已打开的 CPC3788 串口读一轮数据，返回字段名->数值（可绘图的为 float）。"""
    out = {}
    # RALL: concentration, errors, sat_temp, cond_temp, optics_temp, cabinet_temp, ambient_pressure, orifice_press, nozzle_press, laser_current, liquid_level
    rall = _cpc3788_send_command(ser, "RALL")
    if rall:
        parts = [p.strip() for p in rall.split(",")]
        if len(parts) >= 11:
            try:
                out["concentration"] = float(parts[0])
            except (ValueError, TypeError):
                pass
            try:
                out["optics_temp"] = float(parts[4])
            except (ValueError, TypeError):
                pass
            try:
                out["cabinet_temp"] = float(parts[5])
            except (ValueError, TypeError):
                pass
            try:
                out["absolute_pressure"] = float(parts[6])
            except (ValueError, TypeError):
                pass
            try:
                out["laser_current"] = float(parts[9])
            except (ValueError, TypeError):
                pass
    for var, cmd in _CPC3788_CMD_MAP.items():
        if var in out:
            continue
        resp = _cpc3788_send_command(ser, cmd)
        if not resp:
            continue
        if var == "liquid_level" and "(" in resp:
            try:
                out["liquid_level"] = float(resp.split("(")[1].replace(")", "").strip())
            except (ValueError, IndexError, TypeError):
                pass
        else:
            try:
                out[var] = float(resp)
            except (ValueError, TypeError):
                pass
    return out


# PCASP-100X：1B02 取数回复 74/114/154/194 字节（10/20/30/40 通道）
PCASP_NUM_BINS = 30
PCASP_DATA_REPLY_LEN = 32 + 4 * PCASP_NUM_BINS + 2  # 154
# 30 通道粒径 (μm)，与 06SPP CSV Sizes 一致；直方图横轴用
PCASP_BIN_SIZES_UM = [
    0.10, 0.11, 0.12, 0.13, 0.14, 0.15, 0.16, 0.17, 0.18, 0.20,
    0.22, 0.24, 0.26, 0.28, 0.30, 0.40, 0.50, 0.60, 0.80, 1.00,
    1.20, 1.40, 1.60, 1.80, 2.00, 2.20, 2.40, 2.60, 2.80, 3.00,
]


def _pcasp_settings_from_raw(raw: Optional[Dict]) -> Dict[str, int]:
    """从 pcasp100x.json 的 pcasp 段读取 channel_count / adc_threshold。"""
    p = (raw or {}).get("pcasp") or {}
    ch = int(p.get("channel_count", PCASP_NUM_BINS))
    if ch not in (10, 20, 30, 40):
        ch = PCASP_NUM_BINS
    return {
        "channel_count": ch,
        "adc_threshold": int(p.get("adc_threshold", 20)),
    }


def _pcasp_checksum_le(data: bytes) -> bytes:
    s = sum(data) & 0xFFFF
    return struct.pack("<H", s)


def pcasp_send_init(ser, pump_on: bool = True, raw: Optional[Dict] = None) -> bool:
    """发送 1B01 初始化命令，pump_on 对应协议 bit 1；等待 06 06。
    channel_count / adc_threshold 从设备 JSON（pcasp 段）读取，默认 30 / 20。
    """
    try:
        if not ser or not ser.is_open:
            return False
        s = _pcasp_settings_from_raw(raw)
        cmd = _pcasp_build_init_cmd(
            s["channel_count"],
            adc_threshold=s["adc_threshold"],
            pump_on=pump_on,
        )
        ser.reset_input_buffer()
        ser.write(cmd)
        time.sleep(0.5)
        r = ser.read(128)
        return len(r) >= 2 and r[:2] == bytes([0x06, 0x06])
    except Exception:
        return False


def read_pcasp_sample(ser) -> Optional[Dict[str, Any]]:
    """发送 1B02 取数，读 74/114/154/194 字节，校验后解析（直方图 Byte Swapped + AD->物理量换算）。"""
    try:
        if not ser or not ser.is_open:
            return None
        cmd = _pcasp_build_get_data_cmd()
        ser.reset_input_buffer()
        ser.write(cmd)
        time.sleep(0.05)
        # 兼容 10/20/30/40 bins
        buf = _pcasp_read_exact(ser, 194, timeout_s=1.5)
        if len(buf) not in (74, 114, 154, 194):
            return None
        bins = {74: 10, 114: 20, 154: 30, 194: 40}[len(buf)]
        out = _pcasp_parse_frame(buf, bins)
        if not out or not out.get("_checksum_ok"):
            return None
        # 展平 bin01..binNN
        for i, v in enumerate(out.get("bin_counts") or []):
            out[f"bin{i+1:02d}"] = v
        _pcasp_ad_to_physical(out)
        out["_bins"] = bins
        out["_sample_time"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        return out
    except Exception:
        return None


def get_plot_vars_from_device(raw: Optional[Dict]) -> List[str]:
    """从设备 JSON 的 raw 中取绘图变量名列表（display.plot_vars 或 data_format.fields 的 name）。"""
    if not raw:
        return []
    display = raw.get("display") or {}
    plot_vars = display.get("plot_vars")
    if isinstance(plot_vars, list) and plot_vars:
        return list(plot_vars)
    fields = (raw.get("data_format") or {}).get("fields") or []
    return [f.get("name") for f in fields if f.get("name")]


def get_all_plot_vars_from_device(raw: Optional[Dict]) -> List[str]:
    """试验阶段：返回设备全部采集变量名（data_format.fields 全列），便于勾选绘图。"""
    if not raw:
        return []
    fields = (raw.get("data_format") or {}).get("fields") or []
    names = [f.get("name") for f in fields if f.get("name")]
    if names:
        return names
    return get_plot_vars_from_device(raw)


# 绘图变量中不显示的字段（协议头、有效性字节、故障字、版本、校验等）
_PLOT_VARS_SKIP = frozenset({
    "BlockHead0", "BlockHead1", "DataValidity",
    "FaultA", "FaultB", "FaultC",
    "SoftwareVersion", "CheckSum",
})


def get_all_plot_vars_with_label(raw: Optional[Dict]) -> List[Tuple[str, str]]:
    """返回 (英文变量名, 显示标签) 列表。GRIMM/PCASP 不勾选变量，中间固定为直方图。"""
    if not raw:
        return []
    if _raw_is_grimm(raw):
        return []
    if _raw_is_pcasp(raw):
        return []
    fields = (raw.get("data_format") or {}).get("fields") or []
    if fields:
        return [
            (f["name"], f"{f['name']} — {f.get('description', '')}".strip(" — ") if f.get("name") else ("", ""))
            for f in fields if f.get("name") and f["name"] not in _PLOT_VARS_SKIP
        ]
    names = [n for n in get_plot_vars_from_device(raw) if n not in _PLOT_VARS_SKIP]
    return [(n, n) for n in names]


def _raw_is_grimm(raw: Optional[Dict]) -> bool:
    """仅根据 raw 判断是否为 GRIMM 设备。"""
    if not raw:
        return False
    did = (raw.get("device_id") or "").upper()
    path = (raw.get("config_path") or "").lower() if isinstance(raw.get("config_path"), str) else ""
    return "GRIMM" in did or "grimm" in path


def _raw_is_pcasp(raw: Optional[Dict]) -> bool:
    """仅根据 raw 判断是否为 PCASP-100X 设备。"""
    if not raw:
        return False
    did = (raw.get("device_id") or "").upper()
    path = (raw.get("config_path") or "").lower() if isinstance(raw.get("config_path"), str) else ""
    return "PCASP" in did or "pcasp" in path


def get_display_fields_for_device(raw: Optional[Dict]) -> List[str]:
    """取右侧 txt 显示的字段名。GRIMM 固定 Ch1..Ch32；PCASP 为标量 + bin01..bin30；GPS 用 time_fields_order；其它用 data_format.fields。"""
    if not raw:
        return []
    if _raw_is_grimm(raw):
        return [f"Ch{i+1}" for i in range(NUM_CHANNELS_GRIMM)]
    if _raw_is_pcasp(raw):
        scalar = [
            "hi_gain_baseline", "mid_gain_baseline", "low_gain_baseline", "sample_flow",
            "laser_ref_voltage", "aux_analog_1", "sheath_flow", "electronics_temp",
        ]
        return scalar + [f"bin{i:02d}" for i in range(1, PCASP_NUM_BINS + 1)]
    gps = raw.get("gps") or {}
    if gps:
        parse_rule = (gps.get("data_format") or {}).get("parse_rule") or {}
        order = parse_rule.get("time_fields_order")
        if isinstance(order, list) and order:
            return list(order) + ["second", "ms"]  # second_ms 拆成 second、ms 便于阅读
    fields = (raw.get("data_format") or {}).get("fields") or []
    return [f.get("name") for f in fields if f.get("name")]


def generate_gps_utc_sim() -> Dict[str, Any]:
    """生成一帧 GPS UTC 模拟数据（year, month, day, hour, minute, second_ms, second, ms）。"""
    now = datetime.now(timezone.utc)
    second_ms = now.second * 1000 + now.microsecond // 1000
    return {
        "year": now.year,
        "month": now.month,
        "day": now.day,
        "hour": now.hour,
        "minute": now.minute,
        "second_ms": second_ms,
        "second": now.second,
        "ms": now.microsecond // 1000,
    }


# 北京区域大致范围（用于 YGDS 模拟 lon/lat）
_BJ_LON_MIN, _BJ_LON_MAX = 116.0, 117.0
_BJ_LAT_MIN, _BJ_LAT_MAX = 39.5, 40.5


def generate_ygds_sim(raw: Optional[Dict]) -> Dict[str, Any]:
    """根据设备 JSON 的 data_format.fields 生成一帧 Y/GDS-4C 模拟数据（物理量，已乘 scale）。lon/lat 限制在北京区域。"""
    out = {}
    if not raw:
        return out
    fields = (raw.get("data_format") or {}).get("fields") or []
    t = time.time()
    for f in fields:
        name = f.get("name")
        if not name or name in ("BlockHead0", "BlockHead1", "DataValidity", "CheckSum"):
            continue
        rng = f.get("range")
        typ = (f.get("type") or "").lower()
        scale = f.get("scale", 1)
        # 经度/纬度限制在北京附近
        if name == "longitude":
            mid = (_BJ_LON_MIN + _BJ_LON_MAX) / 2.0
            span = (_BJ_LON_MAX - _BJ_LON_MIN) * 0.4
            val = mid + span * math.sin(t * 0.3) * 0.5 + span * 0.1 * math.sin(t * 1.7)
            out[name] = round(max(_BJ_LON_MIN, min(_BJ_LON_MAX, val)), 6)
            continue
        if name == "latitude":
            mid = (_BJ_LAT_MIN + _BJ_LAT_MAX) / 2.0
            span = (_BJ_LAT_MAX - _BJ_LAT_MIN) * 0.4
            val = mid + span * math.sin(t * 0.25) * 0.5 + span * 0.1 * math.sin(t * 1.5)
            out[name] = round(max(_BJ_LAT_MIN, min(_BJ_LAT_MAX, val)), 6)
            continue
        if isinstance(rng, list) and len(rng) >= 2 and typ in ("uint8", "uint16", "int16", "float64"):
            mid = (rng[0] + rng[1]) / 2.0
            span = (rng[1] - rng[0]) * 0.08
            variation = span * math.sin(t * 0.3) * 0.5 + span * 0.1 * math.sin(t * 1.7)
            val = mid + variation
            if "int" in typ:
                val = int(round(val))
            elif typ == "float64":
                val = round(val, 6)
            out[name] = val
        elif typ == "bytes":
            out[name] = 0
        elif name in ("FaultA", "FaultB", "FaultC", "WorkCounter", "SoftwareVersion"):
            out[name] = 0
        else:
            out[name] = 0
    return out


def generate_nox_o3_sim(raw: Optional[Dict]) -> Dict[str, Any]:
    """根据设备 JSON 的 simulation.data_template 生成 NOx-O3 模拟数据（nox, o3, channel3）。"""
    out = {}
    if not raw:
        return out
    template = (raw.get("simulation") or {}).get("data_template") or {}
    t = time.time()
    defaults = {"nox": (0, 50, 2), "o3": (20, 80, 3), "channel3": (0, 30, 1)}
    for key, def_tuple in defaults.items():
        cfg = template.get(key) if isinstance(template.get(key), dict) else None
        if cfg:
            mn = cfg.get("min", def_tuple[0])
            mx = cfg.get("max", def_tuple[1])
            var = cfg.get("variation", def_tuple[2])
        else:
            mn, mx, var = def_tuple
        mid = (mn + mx) / 2.0
        out[key] = round(mid + var * math.sin(t * 0.5) + 0.3 * var * math.sin(t * 1.3), 2)
    return out


def generate_cpc3788_sim(raw: Optional[Dict]) -> Dict[str, Any]:
    """根据设备 JSON 的 simulation.data_template 与 data_format.fields 生成 CPC3788 模拟数据。"""
    out = {}
    if not raw:
        return out
    template = (raw.get("simulation") or {}).get("data_template") or {}
    t = time.time()

    def _v(cfg_key: str, default_mid: float, default_span: float, decimals: int = 2) -> float:
        cfg = template.get(cfg_key) if isinstance(template.get(cfg_key), dict) else None
        if cfg:
            mn = cfg.get("min", default_mid - default_span / 2)
            mx = cfg.get("max", default_mid + default_span / 2)
            var = cfg.get("variation", default_span * 0.1)
        else:
            mn, mx = default_mid - default_span / 2, default_mid + default_span / 2
            var = default_span * 0.1
        mid = (mn + mx) / 2.0
        return round(mid + var * math.sin(t * 0.4) + 0.2 * var * math.sin(t * 1.1), decimals)

    out["concentration"] = _v("concentration", 2550, 4900, 1)
    out["cabinet_temp"] = _v("cabinet_temp", 25, 10, 2)
    out["inlet_flow"] = round(1.0 + 0.3 * math.sin(t * 0.3), 2)
    out["sample_flow"] = round(500 + 200 * math.sin(t * 0.35), 1)
    out["conditioner_temp"] = round(24 + 2 * math.sin(t * 0.4), 2)
    out["growth_tube_temp"] = round(26 + 1.5 * math.sin(t * 0.45), 2)
    out["optics_temp"] = round(25 + 1 * math.sin(t * 0.5), 2)
    out["absolute_pressure"] = round(95 + 5 * math.sin(t * 0.2), 2)
    out["laser_current"] = int(65 + 8 * math.sin(t * 0.25))
    out["liquid_level"] = int(2000 + 400 * math.sin(t * 0.15))
    out["version"] = "Sim 1.0"
    return out


def _device_is_ygds(row: Dict) -> bool:
    """模块级：判断设备行是否为 YGDS（供 ISO/CVI 取源）。"""
    raw = row.get("raw") or {}
    did = (raw.get("device_id") or "").upper()
    path = (row.get("config_path") or "").lower()
    return "YGDS" in did or "ygds" in path


def get_iso_cvi_received_from_ygds(device_rows: List[Dict]) -> Optional[Dict[str, Any]]:
    """从已模拟中的 YGDS 取 TAS(Vt) 与温度(Tt)，作为 ISO/CVI 的「接收」数据。无 YGDS 模拟时返回 None。"""
    for r in device_rows:
        if not r.get("sim_active") or not _device_is_ygds(r):
            continue
        ld = r.get("last_data")
        if not ld:
            continue
        vt = ld.get("Vt")
        tt = ld.get("Tt")
        if vt is not None and tt is not None:
            try:
                return {"airspext": float(vt), "oatmpext": float(tt)}
            except (TypeError, ValueError):
                pass
    return None


def get_tas_temperature_for_iso_cvi(device_rows: List[Dict]) -> Tuple[float, float, bool]:
    """
    从任意已连接或模拟中的 YGDS 取 TAS(Vt) 与总温(Tt)，供向 CVI/ISO 串口发送。
    返回 (tas_mps, temp_celsius, valid)。无有效数据时 valid=False，tas/temp 为 0。
    """
    for r in device_rows:
        if not _device_is_ygds(r):
            continue
        ld = r.get("last_data")
        if not ld:
            continue
        vt = ld.get("Vt")
        tt = ld.get("Tt")
        if vt is not None and tt is not None:
            try:
                return float(vt), float(tt), True
            except (TypeError, ValueError):
                pass
    return 0.0, 0.0, False


def generate_grimm_sim(raw: Optional[Dict]) -> Dict[str, Any]:
    """GRIMM 模拟数据，约 6s 一次；输出 Ch1..Ch32（与实机 C/c 四行 32 通道一致），用于直方图与 CSV。"""
    out = {}
    if not raw:
        return out
    t = time.time()
    # 模拟实机量级：低通道数大、高通道数小，带小幅波动
    for i in range(NUM_CHANNELS_GRIMM):
        # 近似 5000 -> 100 的衰减曲线 + 正弦扰动
        base = 5000.0 * math.exp(-0.12 * (i + 1)) + 50
        out[f"Ch{i+1}"] = max(0, round(base + 80 * math.sin(t * 0.3 + i * 0.2)))
    return out


def generate_pcasp_sim(raw: Optional[Dict]) -> Dict[str, Any]:
    """PCASP-100X 模拟数据，与 data_format.fields 一致，用于右侧显示与 CSV。"""
    out = {}
    if not raw:
        return out
    t = time.time()
    sim = (raw.get("simulation") or {}).get("data_template") or {}
    sf_cfg = sim.get("sample_flow") or {}
    out["hi_gain_baseline"] = int(1900 + 100 * math.sin(t * 0.2))
    out["mid_gain_baseline"] = int(2000 + 100 * math.sin(t * 0.25))
    out["low_gain_baseline"] = int(2100 + 100 * math.sin(t * 0.22))
    out["sample_flow"] = int(
        (sf_cfg.get("min", 2500) + sf_cfg.get("max", 2600)) / 2
        + (sf_cfg.get("variation", 100) or 100) * math.sin(t * 0.3)
    )
    out["laser_ref_voltage"] = int(3900 + 80 * math.sin(t * 0.15))
    out["aux_analog_1"] = int(2048 + 50 * math.sin(t * 0.12))
    out["sheath_flow"] = int(3000 + 80 * math.sin(t * 0.18))
    # 协议为 AD 计数值，模拟约 2200~2400
    out["electronics_temp"] = int(2300 + 100 * math.sin(t * 0.2))
    bins = [max(0, int(50 + 30 * math.sin(t * 0.4 + i * 0.3))) for i in range(PCASP_NUM_BINS)]
    out["bin_counts"] = bins
    for i in range(PCASP_NUM_BINS):
        out[f"bin{i+1:02d}"] = bins[i]
    _pcasp_ad_to_physical(out)
    out["_bins"] = PCASP_NUM_BINS
    out["_sample_time"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    return out


def generate_iso_cvi_sim(raw: Optional[Dict]) -> Dict[str, Any]:
    """无 YGDS 模拟时，本地生成 ISO/CVI 接收的 TAS 与温度（airspext, oatmpext）。"""
    out = {}
    if not raw:
        return out
    template = (raw.get("simulation") or {}).get("data_template") or {}
    t = time.time()
    # airspext: TAS m/s, oatmpext: 外界温度 ℃
    for key, (def_lo, def_hi, def_var) in [
        ("airspext", (0, 120, 15)),
        ("oatmpext", (0, 24, 3)),
    ]:
        cfg = template.get(key) if isinstance(template.get(key), dict) else None
        if cfg:
            lo, hi = cfg.get("min", def_lo), cfg.get("max", def_hi)
            var = cfg.get("variation", def_var)
        else:
            lo, hi, var = def_lo, def_hi, def_var
        mid = (lo + hi) / 2.0
        out[key] = round(mid + var * math.sin(t * 0.35) + 0.2 * var * math.sin(t * 1.2), 2)
    return out


def get_serial_config_from_device(raw: Dict[str, Any]) -> tuple:
    """
    从设备 JSON 的 raw 中取出串口配置及在 JSON 中的路径。
    返回 (config_dict, config_key_path)。
    config_key_path 用于保存时写回，如 ["config"] 或 ["gps", "config"]。
    """
    if "config" in raw and isinstance(raw["config"], dict):
        return dict(raw["config"]), ["config"]
    if "gps" in raw and isinstance(raw["gps"], dict) and "config" in raw["gps"]:
        return dict(raw["gps"]["config"]), ["gps", "config"]
    return {}, []


def load_devices_list(devices_json_path: str) -> List[Dict]:
    """从 devices.json 加载设备列表。"""
    path = os.path.abspath(devices_json_path)
    if not os.path.isfile(path):
        return []
    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)
    return data.get("devices", [])


def load_device_json(config_path: str, base_dir: str) -> Optional[Dict]:
    """加载单个设备 JSON；config_path 可为相对路径。"""
    if not os.path.isabs(config_path):
        path = os.path.join(base_dir, config_path)
    else:
        path = config_path
    if not os.path.isfile(path):
        return None
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def save_device_config(config_path: str, base_dir: str, raw: Dict, config_key_path: List[str], new_config: Dict) -> bool:
    """将 new_config 写回设备 JSON 的 config_key_path 位置。"""
    if not os.path.isabs(config_path):
        path = os.path.join(base_dir, config_path)
    else:
        path = config_path
    d = raw
    for key in config_key_path[:-1]:
        d = d.setdefault(key, {})
    d[config_key_path[-1]] = new_config
    try:
        with open(path, "w", encoding="utf-8") as f:
            json.dump(raw, f, ensure_ascii=False, indent=2)
        return True
    except Exception:
        return False


# ---------- Realtime Flight Track Map (shapefile basemap) ----------
# 优先使用五省 subset，若存在则不再加载全国 CHN_adm3
SHAPEFILE_5PROVINCES = "chn_adm3_5provinces.shp"


def _load_all_shapefiles(resources_dir: str) -> List[Any]:
    """Load shapefile(s) under resources_dir. Prefer chn_adm3_5provinces.shp if present."""
    try:
        import geopandas as gpd
    except ImportError:
        return []
    if not os.path.isdir(resources_dir):
        return []
    five_prov = os.path.join(resources_dir, SHAPEFILE_5PROVINCES)
    if os.path.isfile(five_prov):
        try:
            return [gpd.read_file(five_prov)]
        except Exception:
            pass
    shp_files = [os.path.join(resources_dir, f) for f in os.listdir(resources_dir) if f.lower().endswith(".shp")]
    out = []
    for p in shp_files:
        try:
            out.append(gpd.read_file(p))
        except Exception:
            pass
    return out


class FlightTrackMapWidget(FigureCanvas):
    """Permanent lon/lat real-time track map with all shapefiles in resources as basemap."""

    def __init__(self, parent=None, resources_dir: str = None, width=6, height=5, dpi=100):
        self.fig = Figure(figsize=(width, height), dpi=dpi)
        super().__init__(self.fig)
        self.setParent(parent)
        self.ax = self.fig.add_subplot(111)
        self.resources_dir = resources_dir or os.path.join(SCRIPT_DIR, "resources")
        self.base_gdfs: List[Any] = []
        self.base_gdfs = _load_all_shapefiles(self.resources_dir)
        self.track: List[Tuple[float, float]] = []
        self.track_line = None
        self.current_point = None
        self.track_position = True
        self.center_lon, self.center_lat = 116.4, 39.9
        self.range_deg = 2.0
        self._draw_basemap()
        self._setup_plot()

    def _draw_basemap(self) -> None:
        drawn = False
        for gdf in self.base_gdfs:
            if gdf is None or gdf.empty:
                continue
            try:
                gdf.plot(ax=self.ax, color="lightgray", edgecolor="darkgray", linewidth=0.5, alpha=0.7)
                drawn = True
            except Exception:
                pass
        if not drawn:
            self._draw_reference_grid()

    def _draw_reference_grid(self) -> None:
        self.ax.plot([self.center_lon], [self.center_lat], "k+", markersize=10)
        self.ax.grid(True, linestyle="--", alpha=0.5)

    def _setup_plot(self) -> None:
        self.ax.set_xlim(self.center_lon - self.range_deg, self.center_lon + self.range_deg)
        self.ax.set_ylim(self.center_lat - self.range_deg, self.center_lat + self.range_deg)
        self.ax.set_xlabel("Longitude (deg)")
        self.ax.set_ylabel("Latitude (deg)")
        self.ax.set_title("Realtime Flight Track")
        self.ax.set_aspect("equal")
        self.ax.tick_params(labelsize=8)
        self.fig.tight_layout()
        self.draw_idle()

    def update_track(self, lon: float, lat: float) -> None:
        if lon == 0 and lat == 0:
            return
        self.track.append((float(lon), float(lat)))
        if len(self.track) > 500:
            self.track.pop(0)
        if self.current_point is None:
            self.current_point = self.ax.scatter(lon, lat, c="red", s=80, marker="^", zorder=5)
        else:
            self.current_point.set_offsets([[lon, lat]])
        if len(self.track) > 1:
            xs = [p[0] for p in self.track]
            ys = [p[1] for p in self.track]
            if self.track_line is None:
                self.track_line, = self.ax.plot(xs, ys, "r-", linewidth=1.2, alpha=0.7, zorder=3)
            else:
                self.track_line.set_data(xs, ys)
        if self.track_position and len(self.track) > 0:
            xlim = self.ax.get_xlim()
            ylim = self.ax.get_ylim()
            margin = 0.2
            xm = (xlim[1] - xlim[0]) * margin
            ym = (ylim[1] - ylim[0]) * margin
            if (lon < xlim[0] + xm or lon > xlim[1] - xm or lat < ylim[0] + ym or lat > ylim[1] - ym):
                xr, yr = xlim[1] - xlim[0], ylim[1] - ylim[0]
                self.ax.set_xlim(lon - xr / 2, lon + xr / 2)
                self.ax.set_ylim(lat - yr / 2, lat + yr / 2)
        self.draw_idle()

    def clear_tracks(self) -> None:
        self.track.clear()
        if self.track_line is not None:
            try:
                self.track_line.remove()
            except Exception:
                pass
            self.track_line = None
        if self.current_point is not None:
            try:
                self.current_point.remove()
            except Exception:
                pass
            self.current_point = None
        self.draw_idle()

    def zoom_in(self) -> None:
        xlim, ylim = self.ax.get_xlim(), self.ax.get_ylim()
        cx = (xlim[0] + xlim[1]) / 2
        cy = (ylim[0] + ylim[1]) / 2
        if self.track:
            cx, cy = self.track[-1][0], self.track[-1][1]
        xr, yr = (xlim[1] - xlim[0]) * 0.8, (ylim[1] - ylim[0]) * 0.8
        self.ax.set_xlim(cx - xr / 2, cx + xr / 2)
        self.ax.set_ylim(cy - yr / 2, cy + yr / 2)
        self.draw_idle()

    def zoom_out(self) -> None:
        xlim, ylim = self.ax.get_xlim(), self.ax.get_ylim()
        cx = (xlim[0] + xlim[1]) / 2
        cy = (ylim[0] + ylim[1]) / 2
        if self.track:
            cx, cy = self.track[-1][0], self.track[-1][1]
        xr, yr = (xlim[1] - xlim[0]) * 1.25, (ylim[1] - ylim[0]) * 1.25
        self.ax.set_xlim(cx - xr / 2, cx + xr / 2)
        self.ax.set_ylim(cy - yr / 2, cy + yr / 2)
        self.draw_idle()


class UnifiedCollectorWindow(QMainWindow):
    def __init__(self, devices_json_path: str = None):
        super().__init__()
        self.setWindowTitle("多仪器统一采集 — 左中右布局")
        self.setMinimumSize(900, 500)
        self.resize(1000, 600)

        self._base_dir = SCRIPT_DIR
        self._devices_json_path = devices_json_path or DEFAULT_DEVICES_JSON
        self._devices_list: List[Dict] = []
        self._device_rows: List[Dict] = []  # 每行: id, config_path, raw, config, config_key_path, serial, port_combo, baud_combo, conn_btn, status_label
        self._selected_row = -1
        self._plot_var_checkboxes: List[Tuple[str, QCheckBox]] = []
        self._plot_var_container: Optional[QWidget] = None
        self._plot_data: Dict[str, deque] = {}
        self._plot_start_time: Optional[float] = None
        self._plot_frames: List[Dict[str, Any]] = []  # 每个勾选变量对应一幅图: {var, widget, fig, canvas, ax, line}
        self._device_checked_vars: Dict[int, set] = {}  # row_index -> set of checked var names
        self._device_plot_data: Dict[int, Dict[str, deque]] = {}  # row_index -> {var: deque(...)}
        self._device_plot_start_time: Dict[int, Optional[float]] = {}  # row_index -> start time

        self._build_ui()
        self._load_devices()
        self._plot_timer = QTimer(self)
        self._plot_timer.timeout.connect(self._poll_and_plot)
        self._plot_timer.start(PLOT_POLL_MS)
        self._grimm_poll_timer = QTimer(self)
        self._grimm_poll_timer.timeout.connect(self._poll_grimm_serial)
        self._grimm_poll_timer.start(100)
        self._sim_timer = QTimer(self)
        self._sim_timer.timeout.connect(self._on_sim_tick)
        self._right_value_labels: Dict[str, QLabel] = {}
        self._flight_track_map: Optional[FlightTrackMapWidget] = None
        self._map_tab_index = 1
        self._last_map_update_time: float = 0
        self._map_update_interval = 1.0

        # 自动保存 CSV：.\data\yyyymmdd_hhmmss\ 下按设备分文件，文件名含时间戳
        self._session_data_dir: Optional[str] = None
        self._session_timestamp: Optional[str] = None
        self._device_csv_writers: Dict[int, Any] = {}  # row_index -> csv.writer 使用的打开文件
        self._device_csv_headers_done: Dict[int, bool] = {}
        self._grimm_P_csv_fh: Optional[Any] = None
        self._grimm_P_csv_writer: Optional[Any] = None
        self._grimm_P_csv_headers_done: bool = False
        self._grimm_P_csv_by_device: Dict[str, Any] = {}  # 仪器设备名 -> {"fh","writer","headers_done"}，Grimm P 按设备名分文件
        self._nox_temp_csv_fh: Optional[Any] = None
        self._nox_temp_csv_writer: Optional[Any] = None
        self._nox_temp_csv_headers_done: bool = False
        self._nox_temp_csv_by_device: Dict[str, Any] = {}  # 仪器设备名 -> {"fh","writer","headers_done"}，NOx 按设备名分文件
        self._ensure_session_data_dir()

    def _ensure_session_data_dir(self) -> None:
        """确保 .\\data 存在并创建本次会话子目录 yyyymmdd_hhmmss。"""
        data_dir = os.path.join(self._base_dir, "data")
        os.makedirs(data_dir, exist_ok=True)
        self._session_timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        self._session_data_dir = os.path.join(data_dir, self._session_timestamp)
        os.makedirs(self._session_data_dir, exist_ok=True)
        if hasattr(self, "_data_path_label"):
            self._data_path_label.setText(f"数据保存: {self._session_data_dir}")

    def _get_device_display_name(self, row: Dict) -> str:
        """仪器设备名：优先 JSON 的 device_id / name，否则用 config 文件名（无扩展名）。用于 CSV 文件名 仪器设备名_yyyymmdd_hhmmss.csv。"""
        raw = row.get("raw") or {}
        name = raw.get("device_id") or raw.get("name")
        if name:
            return str(name).strip()
        cp = row.get("config_path") or ""
        if cp:
            return os.path.splitext(os.path.basename(cp))[0].strip() or "device"
        return "device"

    # 各仪器 CSV 写入：见本文件顶部 docstring。通用设备→本方法；Grimm P→_append_grimm_P_csv；NOx→_append_nox_temp_csv。
    def _append_device_csv(self, row_index: int, row: Dict[str, Any]) -> None:
        """将当前设备的 last_data 追加写入该设备对应的 CSV（表头与列顺序按 JSON 配置）。文件名：仪器设备名_yyyymmdd_hhmmss.csv，仪器名从 JSON 或 json 文件名取。"""
        if not row.get("last_data") or not self._session_data_dir:
            return
        raw = row.get("raw")
        if not raw:
            return
        device_id = self._get_device_display_name(row)
        fields = get_display_fields_for_device(raw)
        if not fields:
            return
        # 表头：优先 display.csv_header_overrides，否则用英文字段名
        overrides = (raw.get("display") or {}).get("csv_header_overrides") or {}
        header_names = [overrides.get(f, f) for f in fields]
        file_path = os.path.join(
            self._session_data_dir,
            f"{device_id}_{self._session_timestamp}.csv"
        )
        try:
            if row_index not in self._device_csv_writers:
                fh = open(file_path, "a", encoding="utf-8-sig", newline="")
                writer = csv.writer(fh)
                self._device_csv_writers[row_index] = (fh, writer)
                self._device_csv_headers_done[row_index] = False
            fh, writer = self._device_csv_writers[row_index]
            if not self._device_csv_headers_done[row_index]:
                writer.writerow(["PC_Time"] + header_names)
                self._device_csv_headers_done[row_index] = True
            pc_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
            values = [str(row["last_data"].get(f, "")) for f in fields]
            writer.writerow([pc_time] + values)
            fh.flush()
        except OSError:
            pass

    def _append_device_plot_point(self, row_index: int, row: Dict, sample: Dict[str, Any], t_now: float) -> None:
        """不论当前是否选中该行，都向该行的 _device_plot_data 追加一个采样点，切换回该设备时曲线不丢段。"""
        if row_index not in self._device_plot_data:
            self._device_plot_data[row_index] = {}
        if self._device_plot_start_time.get(row_index) is None:
            self._device_plot_start_time[row_index] = t_now
        start = self._device_plot_start_time[row_index]
        t_rel = t_now - start
        checked = self._device_checked_vars.get(row_index, set())
        for var in checked:
            if var not in sample or sample.get(var) is None:
                continue
            try:
                val = float(sample[var])
            except (TypeError, ValueError):
                continue
            if var not in self._device_plot_data[row_index]:
                self._device_plot_data[row_index][var] = deque(maxlen=MAX_PLOT_POINTS)
            self._device_plot_data[row_index][var].append((t_rel, val))

    def _append_grimm_P_csv(self, row: Dict, p_values: List[Any]) -> None:
        """将 Grimm P 行（PM2.5/PM10 相关）追加写入 仪器设备名_P_yyyymmdd_hhmmss.csv。"""
        if not p_values or not self._session_data_dir or not self._session_timestamp:
            return
        device_name = self._get_device_display_name(row)
        device_id = (row.get("raw") or {}).get("device_id") or row.get("id") or device_name
        file_path = os.path.join(
            self._session_data_dir,
            f"{device_name}_P_{self._session_timestamp}.csv"
        )
        try:
            entry = self._grimm_P_csv_by_device.get(device_name)
            if entry is None:
                fh = open(file_path, "a", encoding="utf-8-sig", newline="")
                writer = csv.writer(fh)
                self._grimm_P_csv_by_device[device_name] = {"fh": fh, "writer": writer, "headers_done": False}
                entry = self._grimm_P_csv_by_device[device_name]
            if not entry["headers_done"]:
                n = len(p_values)
                entry["writer"].writerow(["PC_Time", "device_id"] + [f"P{i+1}" for i in range(n)])
                entry["headers_done"] = True
            pc_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
            entry["writer"].writerow([pc_time, device_id] + [str(v) for v in p_values])
            entry["fh"].flush()
        except OSError:
            pass

    def _append_nox_temp_csv(self, full_data: Dict[str, Any], row: Dict) -> None:
        """NOx-O3：按附带 APP 的 Temp 格式追加一行。文件名 仪器设备名_yyyymmdd_hhmmss.csv（仪器名从 JSON 或 json 文件名）。"""
        if not full_data or not self._session_data_dir or not self._session_timestamp:
            return
        device_name = self._get_device_display_name(row)
        file_path = os.path.join(
            self._session_data_dir,
            f"{device_name}_{self._session_timestamp}.csv"
        )
        try:
            entry = self._nox_temp_csv_by_device.get(device_name)
            if entry is None:
                fh = open(file_path, "a", encoding="utf-8-sig", newline="")
                writer = csv.writer(fh)
                self._nox_temp_csv_by_device[device_name] = {"fh": fh, "writer": writer, "headers_done": False}
                entry = self._nox_temp_csv_by_device[device_name]
            if not entry["headers_done"]:
                entry["writer"].writerow(NOX_TEMP_CSV_HEADER)
                entry["headers_done"] = True
            pc_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            row_values = [pc_time]
            for f in NOX_TEMP_CSV_FIELDS:
                v = full_data.get(f, "")
                if v is None:
                    v = ""
                row_values.append(v)
            entry["writer"].writerow(row_values)
            entry["fh"].flush()
        except OSError:
            pass

    def _close_device_csv_files(self) -> None:
        """关闭所有已打开的设备 CSV 文件句柄（含 Grimm P 专用 CSV）。"""
        for row_index, pair in list(self._device_csv_writers.items()):
            try:
                pair[0].close()
            except Exception:
                pass
        self._device_csv_writers.clear()
        self._device_csv_headers_done.clear()
        if self._grimm_P_csv_by_device:
            for _name, entry in list(self._grimm_P_csv_by_device.items()):
                try:
                    if entry.get("fh"):
                        entry["fh"].close()
                except Exception:
                    pass
            self._grimm_P_csv_by_device.clear()
        if self._grimm_P_csv_fh is not None:
            try:
                self._grimm_P_csv_fh.close()
            except Exception:
                pass
            self._grimm_P_csv_fh = None
            self._grimm_P_csv_writer = None
            self._grimm_P_csv_headers_done = False
        if self._nox_temp_csv_by_device:
            for _name, entry in list(self._nox_temp_csv_by_device.items()):
                try:
                    if entry.get("fh"):
                        entry["fh"].close()
                except Exception:
                    pass
            self._nox_temp_csv_by_device.clear()
        if self._nox_temp_csv_fh is not None:
            try:
                self._nox_temp_csv_fh.close()
            except Exception:
                pass
            self._nox_temp_csv_fh = None
            self._nox_temp_csv_writer = None
            self._nox_temp_csv_headers_done = False

    def _build_ui(self) -> None:
        central = QWidget()
        self.setCentralWidget(central)
        main_layout = QVBoxLayout(central)

        splitter = QSplitter(Qt.Horizontal)

        # ---------- 左侧：仪器列表 + 串口设置 + 绘图变量 ----------
        left = QWidget()
        left_layout = QVBoxLayout(left)
        left_layout.addWidget(QLabel("仪器列表（连接/断开、串口设置可保存到 JSON；勾选「模拟」后点连接为模拟数据）"))
        self._table = QTableWidget(0, 6)
        self._table.setHorizontalHeaderLabels(["设备", "端口", "波特率", "操作", "模拟", "状态"])
        self._table.horizontalHeader().setSectionResizeMode(0, QHeaderView.ResizeMode.Stretch)
        self._table.horizontalHeader().setSectionResizeMode(1, QHeaderView.ResizeMode.Fixed)
        self._table.horizontalHeader().setSectionResizeMode(2, QHeaderView.ResizeMode.Fixed)
        self._table.horizontalHeader().setSectionResizeMode(3, QHeaderView.ResizeMode.Fixed)
        self._table.horizontalHeader().setSectionResizeMode(4, QHeaderView.ResizeMode.Fixed)
        self._table.horizontalHeader().setSectionResizeMode(5, QHeaderView.ResizeMode.Stretch)
        self._table.setColumnWidth(1, 72)
        self._table.setColumnWidth(2, 56)
        self._table.setColumnWidth(3, 48)
        self._table.setColumnWidth(4, 36)
        self._table.setSelectionBehavior(QTableWidget.SelectionBehavior.SelectRows)
        self._table.setSelectionMode(QTableWidget.SelectionMode.SingleSelection)
        self._table.itemSelectionChanged.connect(self._on_table_selection_changed)
        left_layout.addWidget(self._table)
        refresh_btn = QPushButton("刷新串口列表")
        refresh_btn.clicked.connect(self._refresh_serial_ports)
        left_layout.addWidget(refresh_btn)
        save_btn = QPushButton("将当前表格中的端口/波特率保存到各设备 JSON")
        save_btn.clicked.connect(self._save_all_configs_to_json)
        left_layout.addWidget(save_btn)
        # 绘图变量（选中设备后下方显示，试验阶段全列 data_format.fields，可垂直滚动）
        self._plot_vars_group = QGroupBox("绘图变量 — 未选设备")
        plot_vars_layout = QVBoxLayout(self._plot_vars_group)
        self._plot_var_scroll = QScrollArea()
        self._plot_var_scroll.setWidgetResizable(True)
        self._plot_var_scroll.setMinimumHeight(140)
        self._plot_var_scroll.setMaximumHeight(320)
        self._plot_var_scroll.setVerticalScrollBarPolicy(Qt.ScrollBarPolicy.ScrollBarAsNeeded)
        self._plot_var_scroll.setHorizontalScrollBarPolicy(Qt.ScrollBarPolicy.ScrollBarAlwaysOff)
        self._plot_var_container = QWidget()
        self._plot_var_layout = QGridLayout(self._plot_var_container)
        # 下边距加大，避免最后一排 checkbox 被边框或滚动区域裁切
        self._plot_var_layout.setContentsMargins(6, 6, 6, 28)
        self._plot_var_layout.setSpacing(6)
        self._plot_var_layout.setAlignment(Qt.AlignmentFlag.AlignLeft | Qt.AlignmentFlag.AlignTop)
        self._plot_var_scroll.setWidget(self._plot_var_container)
        plot_vars_layout.addWidget(self._plot_var_scroll)
        left_layout.addWidget(self._plot_vars_group)
        # PCASP Pump 开关（仅选中 PCASP 时显示，位于左下）
        self._pcasp_pump_group = QGroupBox("PCASP 泵")
        pcasp_pump_layout = QHBoxLayout(self._pcasp_pump_group)
        self._pcasp_pump_btn = QPushButton("OFF")
        self._pcasp_pump_btn.setCheckable(True)
        self._pcasp_pump_btn.setMinimumWidth(72)
        self._pcasp_pump_btn.setStyleSheet("QPushButton:checked { background-color: #2e7d32; color: white; } QPushButton:!checked { background-color: #c62828; color: white; }")
        self._pcasp_pump_btn.clicked.connect(self._on_pcasp_pump_clicked)
        pcasp_pump_layout.addWidget(QLabel("Pump:"))
        pcasp_pump_layout.addWidget(self._pcasp_pump_btn)
        pcasp_pump_layout.addStretch(1)
        left_layout.addWidget(self._pcasp_pump_group)
        self._pcasp_pump_group.setVisible(False)
        splitter.addWidget(left)

        # ---------- 中间：Tab 1 实时曲线 / Tab 2 Realtime Flight Track map ----------
        center = QGroupBox("中间 — 曲线与轨迹图")
        center_layout = QVBoxLayout(center)
        self._center_tabs = QTabWidget()
        # Tab 1: 每变量一图
        tab_plots = QWidget()
        tab_plots_layout = QVBoxLayout(tab_plots)
        self._center_scroll = QScrollArea()
        self._center_scroll.setWidgetResizable(True)
        self._center_scroll.setHorizontalScrollBarPolicy(Qt.ScrollBarPolicy.ScrollBarAlwaysOff)
        self._center_scroll.setVerticalScrollBarPolicy(Qt.ScrollBarPolicy.ScrollBarAsNeeded)
        self._center_scroll.setMinimumHeight(280)
        self._center_content = QWidget()
        self._center_layout = QVBoxLayout(self._center_content)
        self._center_layout.setContentsMargins(4, 4, 4, 4)
        self._center_layout.setSpacing(8)
        self._center_scroll.setWidget(self._center_content)
        tab_plots_layout.addWidget(self._center_scroll)
        self._center_tabs.addTab(tab_plots, "Realtime curves")
        # Tab 2: Realtime Flight Track map (lazy load to speed startup)
        self._tab_map = QWidget()
        self._tab_map_layout = QVBoxLayout(self._tab_map)
        self._map_placeholder = QLabel("Realtime Flight Track map — open this tab to load (reduces startup time)")
        self._map_placeholder.setAlignment(Qt.AlignmentFlag.AlignCenter)
        self._tab_map_layout.addWidget(self._map_placeholder)
        map_btn_layout = QHBoxLayout()
        zoom_in_btn = QPushButton("Zoom In")
        zoom_in_btn.clicked.connect(self._on_map_zoom_in)
        zoom_out_btn = QPushButton("Zoom Out")
        zoom_out_btn.clicked.connect(self._on_map_zoom_out)
        clear_track_btn = QPushButton("Clear Track")
        clear_track_btn.clicked.connect(self._on_map_clear_track)
        map_btn_layout.addWidget(zoom_in_btn)
        map_btn_layout.addWidget(zoom_out_btn)
        map_btn_layout.addWidget(clear_track_btn)
        map_btn_layout.addStretch()
        self._tab_map_layout.addLayout(map_btn_layout)
        self._center_tabs.addTab(self._tab_map, "Realtime Flight Track map")
        self._center_tabs.currentChanged.connect(self._on_center_tab_changed)
        center_layout.addWidget(self._center_tabs)
        splitter.addWidget(center)

        # ---------- 右侧：实时数据（txt 显示，如 GPS UTC 字段）；PCASP 时加宽以显示 raw+scaled ----------
        right = QGroupBox("右侧 — 实时数据")
        self._right_group = right
        right.setMaximumWidth(280)
        right_layout = QVBoxLayout(right)
        # PCASP 需要右侧 txt 显示 raw + scaled（选中 PCASP 时显示）
        self._right_text = QPlainTextEdit()
        self._right_text.setReadOnly(True)
        self._right_text.setVisible(False)
        self._right_scroll = QScrollArea()
        self._right_scroll.setWidgetResizable(True)
        self._right_content = QWidget()
        self._right_form = QFormLayout(self._right_content)
        self._right_form.setSpacing(2)
        self._right_scroll.setWidget(self._right_content)
        right_layout.addWidget(self._right_text)
        right_layout.addWidget(self._right_scroll)
        splitter.addWidget(right)

        splitter.setSizes([240, 520, 240])
        main_layout.addWidget(splitter)

        self._status_bar = QStatusBar()
        self.setStatusBar(self._status_bar)
        self._status_bar.showMessage("就绪 — 从 config/devices.json 加载设备")
        self._data_path_label = QLabel("")
        self._data_path_label.setStyleSheet("color: #666;")
        self._status_bar.addPermanentWidget(self._data_path_label)

    def _load_devices(self) -> None:
        self._devices_list = load_devices_list(self._devices_json_path)
        enabled = [d for d in self._devices_list if d.get("enabled", True)]
        self._device_rows.clear()
        self._table.setRowCount(len(enabled))
        for row, dev in enumerate(enabled):
            config_path = dev.get("config_path", "")
            raw = load_device_json(config_path, self._base_dir)
            if raw is None:
                name = dev.get("id", "") + " (加载失败)"
                config, config_key_path = {}, []
            else:
                name = raw.get("name", dev.get("id", ""))
                config, config_key_path = get_serial_config_from_device(raw)
                if not config and raw.get("type") == "gps_ntp":
                    config, config_key_path = get_serial_config_from_device(raw)

            port = config.get("port", "COM1")
            baud = str(config.get("baudrate", 115200))

            self._table.setItem(row, 0, QTableWidgetItem(name))
            port_combo = QComboBox()
            port_combo.setMinimumWidth(68)
            self._fill_port_combo(port_combo, port)
            self._table.setCellWidget(row, 1, port_combo)
            baud_combo = QComboBox()
            baud_combo.addItems(DEFAULT_BAUDRATES)
            baud_combo.setCurrentText(baud)
            baud_combo.setMinimumWidth(52)
            self._table.setCellWidget(row, 2, baud_combo)
            conn_btn = QPushButton("连接")
            conn_btn.setMinimumWidth(44)
            conn_btn.clicked.connect(lambda checked=False, r=row: self._on_connect_clicked(r))
            self._table.setCellWidget(row, 3, conn_btn)
            sim_checkbox = QCheckBox("模拟")
            sim_checkbox.setToolTip("勾选后点「连接」将使用模拟数据，不占用串口")
            self._table.setCellWidget(row, 4, sim_checkbox)
            status_label = QLabel("未连接")
            status_label.setStyleSheet("color: gray;")
            self._table.setCellWidget(row, 5, status_label)

            self._device_rows.append({
                "id": dev.get("id"),
                "config_path": config_path,
                "raw": raw,
                "config": config,
                "config_key_path": config_key_path,
                "serial": None,
                "port_combo": port_combo,
                "baud_combo": baud_combo,
                "conn_btn": conn_btn,
                "sim_checkbox": sim_checkbox,
                "sim_active": False,
                "last_data": None,
                "status_label": status_label,
            })
        self._status_bar.showMessage(f"已加载 {len(enabled)} 台设备")

    def _fill_port_combo(self, combo: QComboBox, current: str) -> None:
        ports = [(p.device, f"{p.device} - {p.description or 'Unknown'}") for p in serial.tools.list_ports.comports()]
        combo.clear()
        if not ports:
            combo.addItem("无可用串口", None)
        else:
            for dev, desc in ports:
                combo.addItem(desc, dev)
        idx = combo.findData(current)
        if idx >= 0:
            combo.setCurrentIndex(idx)
        else:
            idx = combo.findText(current, Qt.MatchFlag.MatchContains)
            if idx >= 0:
                combo.setCurrentIndex(idx)

    def _refresh_serial_ports(self) -> None:
        for row in self._device_rows:
            port_combo = row["port_combo"]
            current = port_combo.currentData() or (port_combo.currentText().split()[0] if port_combo.currentText() else None)
            self._fill_port_combo(port_combo, current or row["config"].get("port", "COM1"))
        self._status_bar.showMessage("已刷新串口列表")

    def _get_port_baud(self, row_index: int) -> tuple:
        row = self._device_rows[row_index]
        port = row["port_combo"].currentData()
        if not port and row["port_combo"].currentText():
            text = row["port_combo"].currentText().strip()
            if text != "无可用串口":
                port = text.split()[0] if " " in text else text
        try:
            baud = int(row["baud_combo"].currentText())
        except (ValueError, TypeError):
            baud = 115200
        return port, baud

    def _on_connect_clicked(self, row_index: int) -> None:
        row = self._device_rows[row_index]
        # 断开：串口或模拟
        if row.get("sim_active"):
            row["sim_active"] = False
            row["status_label"].setText("未连接")
            row["status_label"].setStyleSheet("color: gray;")
            row["conn_btn"].setText("连接")
            if not any(r.get("sim_active") for r in self._device_rows):
                self._sim_timer.stop()
            return
        if row["serial"] is not None and row["serial"].is_open:
            try:
                row["serial"].close()
            except Exception:
                pass
            row["serial"] = None
            row["status_label"].setText("未连接")
            row["status_label"].setStyleSheet("color: gray;")
            row["conn_btn"].setText("连接")
            if row.get("sim_checkbox"):
                row["sim_checkbox"].setEnabled(True)
            return
        # 连接：若勾选「模拟」且为 GPS，则启动模拟
        if row.get("sim_checkbox") and row["sim_checkbox"].isChecked() and self._is_gps_ntp(row):
            row["sim_active"] = True
            row["last_data"] = generate_gps_utc_sim()
            row["status_label"].setText("模拟中")
            row["status_label"].setStyleSheet("color: blue;")
            row["conn_btn"].setText("断开")
            if not self._sim_timer.isActive():
                self._sim_timer.start(SIM_TICK_MS)
            if row_index == self._selected_row and row["last_data"]:
                self._refresh_right_panel()
            return
        # 连接：若勾选「模拟」且为 YGDS，则启动模拟
        if row.get("sim_checkbox") and row["sim_checkbox"].isChecked() and self._is_ygds(row):
            row["sim_active"] = True
            row["last_data"] = generate_ygds_sim(row.get("raw"))
            row["status_label"].setText("模拟中")
            row["status_label"].setStyleSheet("color: blue;")
            row["conn_btn"].setText("断开")
            if not self._sim_timer.isActive():
                self._sim_timer.start(SIM_TICK_MS)
            if row_index == self._selected_row and row["last_data"]:
                self._refresh_right_panel()
            return
        # 连接：若勾选「模拟」且为 NOx-O3，则启动模拟
        if row.get("sim_checkbox") and row["sim_checkbox"].isChecked() and self._is_nox_o3(row):
            row["sim_active"] = True
            row["last_data"] = generate_nox_o3_sim(row.get("raw"))
            row["status_label"].setText("模拟中")
            row["status_label"].setStyleSheet("color: blue;")
            row["conn_btn"].setText("断开")
            if not self._sim_timer.isActive():
                self._sim_timer.start(SIM_TICK_MS)
            if row_index == self._selected_row and row["last_data"]:
                self._refresh_right_panel()
            return
        # 连接：若勾选「模拟」且为 CPC3788，则启动模拟
        if row.get("sim_checkbox") and row["sim_checkbox"].isChecked() and self._is_cpc3788(row):
            row["sim_active"] = True
            row["last_data"] = generate_cpc3788_sim(row.get("raw"))
            row["status_label"].setText("模拟中")
            row["status_label"].setStyleSheet("color: blue;")
            row["conn_btn"].setText("断开")
            if not self._sim_timer.isActive():
                self._sim_timer.start(SIM_TICK_MS)
            if row_index == self._selected_row and row["last_data"]:
                self._refresh_right_panel()
            return
        # 连接：若勾选「模拟」且为 ISO/CVI，则模拟接收（TAS/温度来自 YGDS 模拟或本地生成）
        if row.get("sim_checkbox") and row["sim_checkbox"].isChecked() and self._is_iso_or_cvi(row):
            row["sim_active"] = True
            row["last_data"] = get_iso_cvi_received_from_ygds(self._device_rows) or generate_iso_cvi_sim(row.get("raw"))
            row["status_label"].setText("模拟接收")
            row["status_label"].setStyleSheet("color: blue;")
            row["conn_btn"].setText("断开")
            if not self._sim_timer.isActive():
                self._sim_timer.start(SIM_TICK_MS)
            if row_index == self._selected_row and row["last_data"]:
                self._refresh_right_panel()
            return
        # 连接：若勾选「模拟」且为 GRIMM，6s 一次数据，收到即保存
        if row.get("sim_checkbox") and row["sim_checkbox"].isChecked() and self._is_grimm(row):
            row["sim_active"] = True
            row["_last_grimm_t"] = 0
            row["last_data"] = generate_grimm_sim(row.get("raw"))
            row["status_label"].setText("模拟中(6s)")
            row["status_label"].setStyleSheet("color: blue;")
            row["conn_btn"].setText("断开")
            if not self._sim_timer.isActive():
                self._sim_timer.start(SIM_TICK_MS)
            if row_index == self._selected_row and row["last_data"]:
                self._refresh_right_panel()
            self._append_device_csv(row_index, row)
            return
        # 连接：若勾选「模拟」且为 PCASP-100X
        if row.get("sim_checkbox") and row["sim_checkbox"].isChecked() and self._is_pcasp(row):
            row["sim_active"] = True
            row["last_data"] = generate_pcasp_sim(row.get("raw"))
            row["status_label"].setText("模拟中")
            row["status_label"].setStyleSheet("color: blue;")
            row["conn_btn"].setText("断开")
            if not self._sim_timer.isActive():
                self._sim_timer.start(SIM_TICK_MS)
            if row_index == self._selected_row and row["last_data"]:
                self._refresh_right_panel()
            self._append_device_csv(row_index, row)
            return
        port, baud = self._get_port_baud(row_index)
        if not port or port == "无可用串口":
            row["status_label"].setText("请选择端口")
            return
        try:
            # GRIMM：与 grimm_simple.py 一致，9600、timeout=1、不启用 xonxoff，否则可能收不到数据
            if self._is_grimm(row):
                ser = serial.Serial(port=port, baudrate=baud, timeout=1)
            else:
                kw = {"port": port, "baudrate": baud, "timeout": 0.5}
                if row.get("config") and row["config"].get("xonxoff"):
                    kw["xonxoff"] = True
                ser = serial.Serial(**kw)
            row["serial"] = ser
            row["status_label"].setText(f"已连接 {port} @ {baud}")
            row["status_label"].setStyleSheet("color: green;")
            row["conn_btn"].setText("断开")
            if row.get("sim_checkbox"):
                row["sim_checkbox"].setEnabled(False)
            # GRIMM 连接成功后自动选中该行，以便右侧 Ch1..Ch32 与中间直方图立即显示
            if self._is_grimm(row) and self._selected_row != row_index:
                self._table.selectRow(row_index)
            # NOx-O3：连接后先发「调试开」再发「启动数采」，设备才开始上传（延迟 1.2s 等设备就绪）
            if self._is_nox_o3(row) and _NOX_PROTOCOL_AVAILABLE:
                QTimer.singleShot(1200, lambda ri=row_index: self._nox_send_start_data_transfer(ri))
            # PCASP-100X：连接后按 JSON pcasp.pump_on_connect 发 1B01，channel/threshold 见 pcasp 段
            if self._is_pcasp(row):
                raw_dev = row.get("raw") or {}
                pcasp_cfg = raw_dev.get("pcasp") or {}
                row["_pcasp_pump_on"] = bool(pcasp_cfg.get("pump_on_connect", True))
                QTimer.singleShot(300, lambda ri=row_index: self._pcasp_init_connected(ri))
            # CVI/ISO：连接后发送配置命令，使设备接收外部 TAS/温度（oatmpsrc=1, airspsrc=1 等）
            if self._is_iso_or_cvi(row) and _ISO_CVI_PROTOCOL_AVAILABLE:
                row["_iso_cvi_last_send_time"] = 0
                QTimer.singleShot(400, lambda ri=row_index: self._iso_cvi_send_config_commands(ri))
        except Exception as e:
            row["status_label"].setText(f"打开失败: {e}")

    def _on_table_selection_changed(self) -> None:
        old_row = self._selected_row
        rows = self._table.selectedIndexes()
        if not rows:
            new_row = -1
        else:
            new_row = rows[0].row()
        if old_row >= 0 and old_row < len(self._device_rows):
            self._device_checked_vars[old_row] = set(self._get_checked_plot_vars())
            self._device_plot_data[old_row] = {k: deque(v) for k, v in self._plot_data.items()}
            self._device_plot_start_time[old_row] = self._plot_start_time
        self._selected_row = new_row
        if new_row >= 0:
            if new_row not in self._device_plot_data:
                self._device_plot_data[new_row] = {}
            self._plot_data = self._device_plot_data[new_row]
            self._plot_start_time = self._device_plot_start_time.get(new_row)
        else:
            self._plot_data = {}
            self._plot_start_time = None
        self._refresh_plot_var_checkboxes()
        self._rebuild_center_plots()
        self._redraw_plot()
        self._refresh_right_panel()

    def _refresh_plot_var_checkboxes(self) -> None:
        for _, cb in self._plot_var_checkboxes:
            if cb.parent():
                cb.setParent(None)
            cb.deleteLater()
        self._plot_var_checkboxes.clear()
        layout = self._plot_var_layout
        while layout.count():
            item = layout.takeAt(0)
            if item.widget():
                item.widget().deleteLater()
        if self._selected_row < 0 or self._selected_row >= len(self._device_rows):
            self._plot_vars_group.setTitle("绘图变量 — 未选设备")
            self._pcasp_pump_group.setVisible(False)
            return
        row = self._device_rows[self._selected_row]
        raw = row.get("raw")
        device_name = (raw or {}).get("name") or row.get("id") or "未选设备"
        self._plot_vars_group.setTitle(f"绘图变量 — {device_name}")
        plot_vars_with_label = get_all_plot_vars_with_label(raw)
        if _raw_is_grimm(raw):
            self._pcasp_pump_group.setVisible(False)
            layout.addWidget(QLabel("GRIMM：中间显示 32 通道直方图，无需勾选。"), 0, 0)
            return
        if _raw_is_pcasp(raw):
            self._pcasp_pump_group.setVisible(True)
            pump_on = row.get("_pcasp_pump_on", True)
            self._pcasp_pump_btn.blockSignals(True)
            self._pcasp_pump_btn.setChecked(pump_on)
            self._pcasp_pump_btn.setText("ON" if pump_on else "OFF")
            self._pcasp_pump_btn.blockSignals(False)
            layout.addWidget(QLabel("PCASP：中间显示直方图，无需勾选。"), 0, 0)
            return
        self._pcasp_pump_group.setVisible(False)
        saved_checked = self._device_checked_vars.get(self._selected_row, set())
        cols = 4
        for i, (var_name, display_label) in enumerate(plot_vars_with_label):
            cb = QCheckBox(display_label)
            cb.setChecked(var_name in saved_checked)
            cb.stateChanged.connect(self._on_plot_var_toggled)
            r, col = i // cols, i % cols
            layout.addWidget(cb, r, col)
            self._plot_var_checkboxes.append((var_name, cb))
        # 按行数设置容器最小高度，保证所有 checkbox 都能显示、不被边框裁切
        if plot_vars_with_label:
            rows = (len(plot_vars_with_label) + cols - 1) // cols
            self._plot_var_container.setMinimumHeight(rows * 28 + 44)
        if not plot_vars_with_label:
            layout.addWidget(QLabel("（该设备无采集变量或未配置 data_format.fields）"), 0, 0)

    def _pcasp_init_connected(self, row_index: int) -> None:
        """连接后延迟初始化：按设备 JSON 的 channel_count / adc_threshold 发 1B01。"""
        if row_index < 0 or row_index >= len(self._device_rows):
            return
        row = self._device_rows[row_index]
        ser = row.get("serial")
        if not ser or not ser.is_open or not self._is_pcasp(row):
            return
        pump_on = bool(row.get("_pcasp_pump_on", True))
        raw_dev = row.get("raw")
        if pcasp_send_init(ser, pump_on=pump_on, raw=raw_dev):
            s = _pcasp_settings_from_raw(raw_dev)
            self._status_bar.showMessage(
                f"PCASP init OK (0606) bins={s['channel_count']} thr={s['adc_threshold']} pump={'ON' if pump_on else 'OFF'}"
            )
        else:
            self._status_bar.showMessage("PCASP 初始化未收到 0606")

    def _on_pcasp_pump_clicked(self) -> None:
        """PCASP Pump 开关：发送 1B01 更新泵状态（bit 1）。"""
        if self._selected_row < 0 or self._selected_row >= len(self._device_rows):
            return
        row = self._device_rows[self._selected_row]
        if not self._is_pcasp(row):
            return
        ser = row.get("serial")
        if not ser or not ser.is_open:
            self._status_bar.showMessage("PCASP 未连接串口，无法切换泵")
            return
        pump_on = self._pcasp_pump_btn.isChecked()
        row["_pcasp_pump_on"] = pump_on
        self._pcasp_pump_btn.setText("ON" if pump_on else "OFF")
        if pcasp_send_init(ser, pump_on=pump_on, raw=row.get("raw")):
            self._status_bar.showMessage(f"PCASP 泵已设为 {'ON' if pump_on else 'OFF'}")
        else:
            self._status_bar.showMessage("PCASP 初始化命令未收到 0606 回复")

    def _on_plot_var_toggled(self) -> None:
        self._rebuild_center_plots()
        self._redraw_plot()

    def _on_sim_tick(self) -> None:
        """模拟定时：为所有「模拟中」的 GPS/YGDS 设备生成数据并刷新右侧/中间图（若选中）。"""
        for row_index, row in enumerate(self._device_rows):
            if not row.get("sim_active"):
                continue
            if self._is_gps_ntp(row):
                row["last_data"] = generate_gps_utc_sim()
                if row_index == self._selected_row and row["last_data"]:
                    self._update_right_panel_values(row["last_data"])
                self._append_device_csv(row_index, row)
                continue
            if self._is_ygds(row):
                row["last_data"] = generate_ygds_sim(row.get("raw"))
                self._append_device_csv(row_index, row)
                t_now = time.time()
                if row["last_data"]:
                    self._append_device_plot_point(row_index, row, row["last_data"], t_now)
                if row_index == self._selected_row and row["last_data"]:
                    if self._plot_start_time is None:
                        self._plot_start_time = t_now
                    self._update_right_panel_values(row["last_data"])
                    self._redraw_plot()
                continue
            if self._is_nox_o3(row):
                row["last_data"] = generate_nox_o3_sim(row.get("raw"))
                self._append_device_csv(row_index, row)
                t_now = time.time()
                if row["last_data"]:
                    self._append_device_plot_point(row_index, row, row["last_data"], t_now)
                if row_index == self._selected_row and row["last_data"]:
                    if self._plot_start_time is None:
                        self._plot_start_time = t_now
                    self._update_right_panel_values(row["last_data"])
                    self._redraw_plot()
                continue
            if self._is_cpc3788(row):
                row["last_data"] = generate_cpc3788_sim(row.get("raw"))
                self._append_device_csv(row_index, row)
                t_now = time.time()
                if row["last_data"]:
                    self._append_device_plot_point(row_index, row, row["last_data"], t_now)
                if row_index == self._selected_row and row["last_data"]:
                    if self._plot_start_time is None:
                        self._plot_start_time = t_now
                    self._update_right_panel_values(row["last_data"])
                    self._redraw_plot()
                continue
            if self._is_pcasp(row):
                row["last_data"] = generate_pcasp_sim(row.get("raw"))
                self._append_device_csv(row_index, row)
                t_now = time.time()
                if row["last_data"]:
                    self._append_device_plot_point(row_index, row, row["last_data"], t_now)
                if row_index == self._selected_row and row["last_data"]:
                    if self._plot_start_time is None:
                        self._plot_start_time = t_now
                    self._update_right_panel_values(row["last_data"])
                    self._redraw_plot()
                continue
            if self._is_iso_or_cvi(row):
                row["last_data"] = get_iso_cvi_received_from_ygds(self._device_rows) or generate_iso_cvi_sim(row.get("raw"))
                self._append_device_csv(row_index, row)
                t_now = time.time()
                if row["last_data"]:
                    self._append_device_plot_point(row_index, row, row["last_data"], t_now)
                if row_index == self._selected_row and row["last_data"]:
                    if self._plot_start_time is None:
                        self._plot_start_time = t_now
                    self._update_right_panel_values(row["last_data"])
                    self._redraw_plot()
                continue
            # GRIMM：6s 一次，收到（模拟/串口）即保存、显示
            if self._is_grimm(row):
                t_now = time.time()
                last_t = row.get("_last_grimm_t") or 0
                if last_t == 0:
                    row["_last_grimm_t"] = t_now
                if t_now - last_t >= 6.0:
                    row["last_data"] = generate_grimm_sim(row.get("raw"))
                    self._append_device_csv(row_index, row)
                    row["_last_grimm_t"] = t_now
                    if row_index == self._selected_row and row["last_data"]:
                        self._update_right_panel_values(row["last_data"])
        self._update_flight_track_map()

    def _on_center_tab_changed(self, index: int) -> None:
        if index != self._map_tab_index or self._flight_track_map is not None:
            return
        self._map_placeholder.setParent(None)
        self._tab_map_layout.removeWidget(self._map_placeholder)
        self._flight_track_map = FlightTrackMapWidget(
            self._tab_map,
            resources_dir=os.path.join(SCRIPT_DIR, "resources"),
            width=6, height=5, dpi=100,
        )
        self._tab_map_layout.insertWidget(0, self._flight_track_map)
        self._map_placeholder.deleteLater()
        pos = self._get_track_lon_lat()
        if pos is not None:
            self._flight_track_map.update_track(pos[0], pos[1])
        self._last_map_update_time = time.time()

    def _on_map_zoom_in(self) -> None:
        if self._flight_track_map:
            self._flight_track_map.zoom_in()

    def _on_map_zoom_out(self) -> None:
        if self._flight_track_map:
            self._flight_track_map.zoom_out()

    def _on_map_clear_track(self) -> None:
        if self._flight_track_map:
            self._flight_track_map.clear_tracks()

    def _get_track_lon_lat(self) -> Optional[Tuple[float, float]]:
        """从任意设备的 last_data 中取 longitude, latitude（优先 YGDS）。用于轨迹图。"""
        for row in self._device_rows:
            ld = row.get("last_data")
            if not ld:
                continue
            lon = ld.get("longitude")
            lat = ld.get("latitude")
            if lon is not None and lat is not None:
                try:
                    return float(lon), float(lat)
                except (TypeError, ValueError):
                    pass
        return None

    def _update_flight_track_map(self) -> None:
        if self._flight_track_map is None:
            return
        if self._center_tabs.currentIndex() != self._map_tab_index:
            return
        now = time.time()
        if now - self._last_map_update_time < self._map_update_interval:
            return
        pos = self._get_track_lon_lat()
        if pos is None:
            return
        self._last_map_update_time = now
        self._flight_track_map.update_track(pos[0], pos[1])

    def _refresh_right_panel(self) -> None:
        """根据当前选中设备重建右侧表单并填入 last_data。"""
        # PCASP：右侧改为 txt 显示 raw + scaled
        if self._selected_row >= 0 and self._selected_row < len(self._device_rows):
            row = self._device_rows[self._selected_row]
            if self._is_pcasp(row):
                self._right_group.setMaximumWidth(440)
                self._right_scroll.setVisible(False)
                self._right_text.setVisible(True)
                last = row.get("last_data") or {}
                self._right_text.setPlainText(self._format_pcasp_right_text(row, last))
                return
        self._right_group.setMaximumWidth(280)
        self._right_scroll.setVisible(True)
        self._right_text.setVisible(False)
        self._right_value_labels.clear()
        while self._right_form.rowCount():
            self._right_form.removeRow(0)
        if self._selected_row < 0 or self._selected_row >= len(self._device_rows):
            self._right_form.addRow(QLabel("选中设备后在此显示实时数据（如 GPS UTC）"))
            return
        row = self._device_rows[self._selected_row]
        raw = row.get("raw")
        fields = get_display_fields_for_device(raw)
        if not fields:
            self._right_form.addRow(QLabel("（该设备无配置显示字段）"))
            return
        last = row.get("last_data") or {}
        for name in fields:
            value_label = QLabel("--")
            value_label.setMinimumWidth(56)
            self._right_form.addRow(QLabel(name + ":"), value_label)
            self._right_value_labels[name] = value_label
        # 立即用 last_data 填一次
        for name, lbl in self._right_value_labels.items():
            lbl.setText(str(last.get(name, "--")))

    def _update_right_panel_values(self, data: Dict[str, Any]) -> None:
        """仅更新右侧已有 value 标签的文本（不重建表单）。"""
        if self._selected_row >= 0 and self._selected_row < len(self._device_rows):
            row = self._device_rows[self._selected_row]
            if self._is_pcasp(row):
                self._right_text.setPlainText(self._format_pcasp_right_text(row, data))
                return
        for name, lbl in self._right_value_labels.items():
            lbl.setText(str(data.get(name, "--")))

    def _format_pcasp_right_text(self, row: Dict[str, Any], d: Dict[str, Any]) -> str:
        """右侧 txt：端口/本帧时间 + raw(AD) + scaled(物理量) + bin。"""
        bins = int(d.get("_bins") or PCASP_NUM_BINS)
        cfg = row.get("config") or {}
        port = cfg.get("port", "—")
        baud = cfg.get("baudrate", "—")
        lines = [
            f"本帧时间: {d.get('_sample_time', '—')}",
            f"配置串口: {port} @ {baud} baud",
            "",
        ]
        # raw
        lines.append("RAW (AD/cts)")
        raw_keys = [
            "hi_gain_baseline", "mid_gain_baseline", "low_gain_baseline",
            "sample_flow", "laser_ref_voltage", "aux_analog_1", "sheath_flow", "electronics_temp",
            "avg_transit", "fifo_full", "reset_flag", "sync_err_a", "sync_err_b", "sync_err_c", "adc_overflow",
        ]
        for k in raw_keys:
            if k in d:
                lines.append(f"{k}: {d.get(k)}")
        lines.append("")
        lines.append("SCALED")
        scaled_map = {
            "hi_gain_baseline_scaled": "hi_gain_baseline_V",
            "mid_gain_baseline_scaled": "mid_gain_baseline_V",
            "low_gain_baseline_scaled": "low_gain_baseline_V",
            "sample_flow_scaled": "sample_flow_std_cm3_s",
            "laser_ref_voltage_scaled": "laser_ref_voltage_V",
            "aux_analog_1_scaled": "aux_analog_1_V",
            "sheath_flow_scaled": "sheath_flow_std_cm3_s",
            "electronics_temp_scaled": "electronics_temp_C",
        }
        for src, name in scaled_map.items():
            if src in d:
                v = d.get(src)
                lines.append(f"{name}: {v}")
        lines.append("")
        lines.append(f"HISTOGRAM bins={bins} (bin01..bin{bins:02d})")
        for i in range(1, bins + 1):
            key = f"bin{i:02d}"
            if key in d:
                lines.append(f"{key}: {d.get(key)}")
        return "\n".join(lines)

    def _get_checked_plot_vars(self) -> List[str]:
        return [var for var, cb in self._plot_var_checkboxes if cb.isChecked()]

    def _rebuild_center_plots(self) -> None:
        """按当前勾选的变量重建中间区。GRIMM 仅显示 32 通道直方图；其它设备为每变量一幅时序图。"""
        layout = self._center_layout
        while layout.count():
            item = layout.takeAt(0)
            if item.widget():
                w = item.widget()
                w.setParent(None)
                w.deleteLater()
        self._plot_frames.clear()
        if self._selected_row >= 0 and self._selected_row < len(self._device_rows):
            row = self._device_rows[self._selected_row]
            if self._is_grimm(row):
                box = QFrame()
                box.setFrameStyle(QFrame.Shape.StyledPanel | QFrame.Shadow.Sunken)
                box_layout = QVBoxLayout(box)
                box_layout.setContentsMargins(4, 4, 4, 4)
                box_layout.addWidget(QLabel("GRIMM 32-channel Histogram"))
                fig = Figure(figsize=(10, 3), dpi=100)
                ax = fig.add_subplot(111)
                ax.set_xlabel("Channel")
                ax.set_ylabel("Count")
                ax.set_title("GRIMM Channel Histogram")
                x = list(range(1, NUM_CHANNELS_GRIMM + 1))
                channels = (row.get("last_data") or {})
                vals = [float(channels.get(f"Ch{i+1}", 0)) for i in range(NUM_CHANNELS_GRIMM)]
                bars = ax.bar(x, vals, color="#1f77b4", edgecolor="#333", linewidth=0.5)
                ax.set_xticks(x)
                max_val = max(vals) if vals else 1
                ax.set_ylim(0, max_val * 1.05 + 1 if max_val > 0 else 1)
                fig.subplots_adjust(left=0.12, right=0.98, top=0.92, bottom=0.14)
                canvas = FigureCanvas(fig)
                box_layout.addWidget(canvas)
                box.setMinimumHeight(220)
                layout.addWidget(box)
                self._plot_frames.append({"grimm_histogram": True, "widget": box, "fig": fig, "canvas": canvas, "ax": ax, "bars": bars})
                return
            if self._is_pcasp(row):
                box = QFrame()
                box.setFrameStyle(QFrame.Shape.StyledPanel | QFrame.Shadow.Sunken)
                box_layout = QVBoxLayout(box)
                box_layout.setContentsMargins(4, 4, 4, 4)
                box_layout.addWidget(QLabel("PCASP 粒径分布（30 档，横轴 μm）"))
                fig = Figure(figsize=(10, 3.2), dpi=100)
                ax = fig.add_subplot(111)
                ax.set_xlabel("粒径 (μm)")
                ax.set_ylabel("计数")
                ax.set_title("PCASP Histogram")
                ld = (row.get("last_data") or {})
                vals = [float(ld.get(f"bin{i:02d}", 0)) for i in range(1, PCASP_NUM_BINS + 1)]
                bars = ax.bar(
                    PCASP_BIN_SIZES_UM,
                    vals,
                    width=0.008,
                    color="#2ca02c",
                    edgecolor="#333",
                    linewidth=0.5,
                )
                ax.set_xlim(-0.05, 3.2)
                max_val = max(vals) if vals else 1
                ax.set_ylim(0, max_val * 1.05 + 1 if max_val > 0 else 1)
                fig.subplots_adjust(left=0.10, right=0.98, top=0.90, bottom=0.18)
                canvas = FigureCanvas(fig)
                box_layout.addWidget(canvas)
                box.setMinimumHeight(240)
                layout.addWidget(box)
                self._plot_frames.append({
                    "pcasp_histogram": True,
                    "widget": box,
                    "fig": fig,
                    "canvas": canvas,
                    "ax": ax,
                    "bars": bars,
                    "pcasp_nbins": PCASP_NUM_BINS,
                })
                return
        checked = self._get_checked_plot_vars()
        for var in checked:
            box = QFrame()
            box.setFrameStyle(QFrame.Shape.StyledPanel | QFrame.Shadow.Sunken)
            box_layout = QVBoxLayout(box)
            box_layout.setContentsMargins(4, 4, 4, 4)
            box_layout.addWidget(QLabel(var))
            fig = Figure(figsize=(4, 2), dpi=100)
            ax = fig.add_subplot(111)
            ax.xaxis.set_major_formatter(mdates.DateFormatter("%H:%M:%S"))
            ax.xaxis.set_major_locator(mdates.AutoDateLocator())
            ax.set_xlabel("Time")
            ax.set_ylabel(var)
            ax.grid(True, alpha=0.3)
            line, = ax.plot([], [], color="#1f77b4", linewidth=1.2)
            canvas = FigureCanvas(fig)
            box_layout.addWidget(canvas)
            box.setMinimumHeight(220)
            layout.addWidget(box)
            self._plot_frames.append({"var": var, "widget": box, "fig": fig, "canvas": canvas, "ax": ax, "line": line})

    def _is_cpc3788(self, row: Dict) -> bool:
        raw = row.get("raw") or {}
        return (raw.get("device_id") or "").upper().startswith("CPC3788") or "cpc3788" in (row.get("config_path") or "").lower()

    def _is_iso_or_cvi(self, row: Dict) -> bool:
        raw = row.get("raw") or {}
        did = (raw.get("device_id") or "").upper()
        path = (row.get("config_path") or "").lower()
        return "ISO" in did or "CVI" in did or "iso" in path or "cvi" in path

    def _is_gps_ntp(self, row: Dict) -> bool:
        raw = row.get("raw") or {}
        return (raw.get("type") == "gps_ntp") or ("gps_ntp" in (row.get("config_path") or "").lower())

    def _is_ygds(self, row: Dict) -> bool:
        raw = row.get("raw") or {}
        did = (raw.get("device_id") or "").upper()
        path = (row.get("config_path") or "").lower()
        return "YGDS" in did or "ygds" in path

    def _is_nox_o3(self, row: Dict) -> bool:
        raw = row.get("raw") or {}
        did = (raw.get("device_id") or "").upper()
        path = (row.get("config_path") or "").lower()
        return "NOX" in did or "nox_o3" in path

    def _iso_cvi_send_config_commands(self, row_index: int) -> None:
        """CVI/ISO：发送配置命令（oatmpsrc=1, airspsrc=1 等），使设备接收外部 TAS/温度。"""
        if row_index < 0 or row_index >= len(self._device_rows):
            return
        row = self._device_rows[row_index]
        ser = row.get("serial")
        if not ser or not ser.is_open or not self._is_iso_or_cvi(row):
            return
        raw = row.get("raw") or {}
        commands = (raw.get("data_format") or {}).get("config_commands")
        if not isinstance(commands, list) or not commands:
            self._status_bar.showMessage("CVI/ISO 未配置 config_commands")
            return
        try:
            for cmd in commands:
                if isinstance(cmd, str) and cmd.strip():
                    if not cmd.endswith("\r"):
                        cmd = cmd.strip() + "\r"
                    ser.write(cmd.encode("ascii"))
                    time.sleep(0.08)
            row["status_label"].setText("已连接 已发送接收外部数据命令")
            self._status_bar.showMessage("CVI/ISO 已发送配置命令（接收外部 TAS/温度）")
        except Exception as e:
            self._status_bar.showMessage(f"CVI/ISO 发送配置命令失败: {e}")

    def _nox_send_start_data_transfer(self, row_index: int) -> None:
        """NOx-O3：先发「调试开」(0x0108)，再发「启动数采」(0x0109)，设备才开始自动上传。与附带 APP 的「调试-开」等效。"""
        if not _NOX_PROTOCOL_AVAILABLE or NOxO3Parser is None or ParamID is None:
            return
        if row_index < 0 or row_index >= len(self._device_rows):
            return
        row = self._device_rows[row_index]
        ser = row.get("serial")
        if not ser or not ser.is_open or not self._is_nox_o3(row):
            return
        try:
            # 1) 先发「调试开」：附带 APP 中必须先点调试才能收数，关 APP 后调试未关故 unified 能收；此处主动发等效命令
            seq = (row.get("_nox_seq", 0) + 1) & 0xFF
            row["_nox_seq"] = seq
            one_u32 = (1).to_bytes(4, byteorder="little")
            if getattr(ParamID, "DEBUG_MODE", None) is not None:
                cmd_debug = NOxO3Parser.build_write_param_command(ParamID.DEBUG_MODE, one_u32, seq)
                ser.write(cmd_debug)
                time.sleep(0.15)
            # 2) 再发「启动数采数据传输」并重试 3 次
            for attempt in range(3):
                seq = (row.get("_nox_seq", 0) + 1) & 0xFF
                row["_nox_seq"] = seq
                cmd = NOxO3Parser.build_write_param_command(ParamID.START_DATA_TRANSFER, one_u32, seq)
                ser.write(cmd)
                row["status_label"].setText(f"已连接 调试+数采命令{'重试%d' % (attempt + 1) if attempt else '已发'}")
                if attempt < 2:
                    time.sleep(0.25)
        except Exception:
            pass

    def _is_grimm(self, row: Dict) -> bool:
        raw = row.get("raw") or {}
        did = (raw.get("device_id") or "").upper()
        path = (row.get("config_path") or "").lower()
        return "GRIMM" in did or "grimm" in path

    def _is_pcasp(self, row: Dict) -> bool:
        raw = row.get("raw") or {}
        did = (raw.get("device_id") or "").upper()
        path = (row.get("config_path") or "").lower()
        return "PCASP" in did or "pcasp" in path

    def _poll_grimm_serial(self) -> None:
        """每 100ms 轮询 Grimm 串口：收齐 4 行 C/c 更新 32ch 与 CSV；P 行单独写入 仪器设备名_P_yyyymmdd_hhmmss.csv。"""
        for row_index, row in enumerate(self._device_rows):
            ser = row.get("serial")
            if not ser or not ser.is_open or not self._is_grimm(row):
                continue
            p_lines: List[List[Any]] = []
            result = read_grimm_frame(ser, row, p_lines_collector=p_lines)
            for p_values in p_lines:
                self._append_grimm_P_csv(row, p_values)
            if not result:
                continue
            row["last_data"], _raw = result
            self._append_device_csv(row_index, row)
            if row_index == self._selected_row:
                self._update_right_panel_values(row["last_data"])
                self._redraw_plot()

    def _poll_and_plot(self) -> None:
        # 1) 轮询所有已连接串口：GPS、YGDS 收到即更新 last_data、保存 CSV、若为当前选中则刷新右侧与曲线
        for row_index, row in enumerate(self._device_rows):
            ser = row.get("serial")
            if not ser or not ser.is_open:
                continue
            if self._is_gps_ntp(row):
                sample = read_gps_timea(ser)
                if sample:
                    row["last_data"] = sample
                    self._append_device_csv(row_index, row)
                    if row_index == self._selected_row:
                        self._update_right_panel_values(row["last_data"])
                continue
            if self._is_ygds(row):
                buf = row.get("_read_buffer")
                if buf is None:
                    buf = bytearray()
                    row["_read_buffer"] = buf
                sample, row["_read_buffer"] = read_ygds_frame(ser, buf)
                if sample:
                    row["last_data"] = sample
                    self._append_device_csv(row_index, row)
                    t_now = time.time()
                    self._append_device_plot_point(row_index, row, sample, t_now)
                    if row_index == self._selected_row:
                        if self._plot_start_time is None:
                            self._plot_start_time = t_now
                        self._update_right_panel_values(row["last_data"])
                        self._redraw_plot()
                continue
            if self._is_nox_o3(row) and _NOX_PROTOCOL_AVAILABLE and NOxO3Parser is not None and NOxConfig is not None:
                buf = row.get("_nox_read_buffer")
                if buf is None:
                    buf = bytearray()
                    row["_nox_read_buffer"] = buf
                if ser.in_waiting:
                    buf.extend(ser.read(ser.in_waiting))
                while len(buf) >= 7:
                    if buf[0] != NOxConfig.PACKET_HEADER:
                        buf.pop(0)
                        continue
                    if len(buf) < 5:
                        break
                    packet_len = int.from_bytes(buf[1:3], byteorder="little")
                    total_len = packet_len + 4
                    if len(buf) < total_len:
                        break
                    packet = bytes(buf[:total_len])
                    del buf[:total_len]
                    if not NOxO3Parser.verify_checksum(packet):
                        continue
                    if len(packet) < 4:
                        continue
                    if packet[3] != 0x01:
                        continue
                    data = NOxO3Parser.parse_auto_upload(packet)
                    if data:
                        data["nox"] = data.get("nox_conc")
                        data["o3"] = data.get("o3_conc")
                        data["channel3"] = data.get("no_conc")
                        row["last_data"] = data
                        self._append_nox_temp_csv(data, row)
                        t_now = time.time()
                        self._append_device_plot_point(row_index, row, data, t_now)
                        if row_index == self._selected_row:
                            if self._plot_start_time is None:
                                self._plot_start_time = t_now
                            self._update_right_panel_values(row["last_data"])
                            self._redraw_plot()
                row["_nox_read_buffer"] = buf
                continue
            if self._is_pcasp(row):
                sample = read_pcasp_sample(ser)
                if sample:
                    row["last_data"] = sample
                    self._append_device_csv(row_index, row)
                    t_now = time.time()
                    self._append_device_plot_point(row_index, row, sample, t_now)
                    if row_index == self._selected_row:
                        if self._plot_start_time is None:
                            self._plot_start_time = t_now
                        self._update_right_panel_values(row["last_data"])
                        self._redraw_plot()
                continue
            if self._is_grimm(row):
                continue
            # CVI/ISO：从 YGDS 取 TAS/温度并发送到设备串口（已在上方连接时发过配置命令）
            if self._is_iso_or_cvi(row) and _ISO_CVI_PROTOCOL_AVAILABLE and encode_tas_temperature_iso_cvi is not None:
                t_now = time.time()
                last_send = row.get("_iso_cvi_last_send_time") or 0
                if t_now - last_send >= ISO_CVI_SEND_INTERVAL:
                    tas, temp, valid = get_tas_temperature_for_iso_cvi(self._device_rows)
                    try:
                        payload = encode_tas_temperature_iso_cvi(tas, temp, valid)
                        ser.write(payload)
                        row["_iso_cvi_last_send_time"] = t_now
                        row["last_data"] = {"airspext": tas, "oatmpext": temp}
                        self._append_device_csv(row_index, row)
                        self._append_device_plot_point(row_index, row, row["last_data"], t_now)
                        if row_index == self._selected_row:
                            if self._plot_start_time is None:
                                self._plot_start_time = t_now
                            self._update_right_panel_values(row["last_data"])
                            self._redraw_plot()
                    except Exception:
                        pass
                continue
        # 2) Grimm 由 _poll_grimm_serial（100ms 定时）单独轮询，与 grimm_simple 类似
        # 3) 当前选中为 CPC3788 且串口已连接时，读 CPC 并刷新右侧/曲线/CSV
        if self._selected_row < 0 or self._selected_row >= len(self._device_rows):
            return
        row = self._device_rows[self._selected_row]
        ser = row.get("serial")
        if not ser or not ser.is_open or not self._is_cpc3788(row):
            return
        sample = read_cpc3788_sample(ser)
        if not sample:
            return
        row["last_data"] = sample
        self._update_right_panel_values(row["last_data"])
        self._append_device_csv(self._selected_row, row)
        t_now = time.time()
        if self._plot_start_time is None:
            self._plot_start_time = t_now
        t_rel = t_now - self._plot_start_time
        checked = self._get_checked_plot_vars()
        for var in checked:
            if var not in sample or sample[var] is None:
                continue
            try:
                val = float(sample[var])
            except (TypeError, ValueError):
                continue
            if var not in self._plot_data:
                self._plot_data[var] = deque(maxlen=MAX_PLOT_POINTS)
            self._plot_data[var].append((t_rel, val))
        self._redraw_plot()

    def _redraw_plot(self) -> None:
        """根据 _plot_data 更新每幅图；GRIMM 为 32 通道直方图，PCASP 为 30 bin 直方图，其它为时序折线。"""
        base_time = self._plot_start_time
        for frame in self._plot_frames:
            if frame.get("grimm_histogram"):
                bars = frame.get("bars")
                canvas = frame["canvas"]
                if bars is not None and self._selected_row >= 0 and self._selected_row < len(self._device_rows):
                    row = self._device_rows[self._selected_row]
                    ld = row.get("last_data") or {}
                    vals = [float(ld.get(f"Ch{i+1}", 0)) for i in range(NUM_CHANNELS_GRIMM)]
                    for i, bar in enumerate(bars):
                        bar.set_height(vals[i] if i < len(vals) else 0)
                    ax = frame["ax"]
                    ax.relim()
                    ax.autoscale_view(scalex=False)
                    max_val = max(vals) if vals else 1
                    ax.set_ylim(0, max_val * 1.05 + 1 if max_val > 0 else 1)
                canvas.draw_idle()
                continue
            if frame.get("pcasp_histogram"):
                bars = frame.get("bars")
                canvas = frame["canvas"]
                if bars is not None and self._selected_row >= 0 and self._selected_row < len(self._device_rows):
                    row = self._device_rows[self._selected_row]
                    ld = row.get("last_data") or {}
                    nb = int(ld.get("_bins") or frame.get("pcasp_nbins") or PCASP_NUM_BINS)
                    max_h = 0.0
                    for i, bar in enumerate(bars):
                        if i < nb:
                            h = float(ld.get(f"bin{i+1:02d}", 0))
                        else:
                            h = 0.0
                        bar.set_height(h)
                        max_h = max(max_h, h)
                    ax = frame["ax"]
                    ax.relim()
                    ax.autoscale_view(scalex=False)
                    ax.set_ylim(0, max_h * 1.05 + 1 if max_h > 0 else 1)
                canvas.draw_idle()
                continue
            var = frame["var"]
            line = frame["line"]
            canvas = frame["canvas"]
            ax = frame["ax"]
            if var not in self._plot_data or not self._plot_data[var]:
                line.set_data([], [])
            else:
                pts = list(self._plot_data[var])
                t_rel = [p[0] for p in pts]
                ys = [p[1] for p in pts]
                if base_time is None and t_rel:
                    base_time = time.time() - max(t_rel)
                if base_time is not None:
                    xs = [mdates.date2num(datetime.fromtimestamp(base_time + t)) for t in t_rel]
                else:
                    xs = t_rel
                line.set_data(xs, ys)
                ax.relim()
                ax.autoscale_view()
            canvas.draw_idle()

    def _save_all_configs_to_json(self) -> None:
        for row in self._device_rows:
            if row["raw"] is None or not row["config_key_path"]:
                continue
            port = row["port_combo"].currentData()
            if not port and row["port_combo"].currentText():
                text = row["port_combo"].currentText().strip()
                if text != "无可用串口":
                    port = text.split()[0] if " " in text else text
            if not port:
                continue
            try:
                baud = int(row["baud_combo"].currentText())
            except (ValueError, TypeError):
                baud = 115200
            new_config = dict(row["config"])
            new_config["port"] = port
            new_config["baudrate"] = baud
            ok = save_device_config(
                row["config_path"],
                self._base_dir,
                row["raw"],
                row["config_key_path"],
                new_config,
            )
            if ok:
                row["config"] = new_config
        self._status_bar.showMessage("已将当前端口/波特率保存到各设备 JSON")
        QMessageBox.information(self, "保存", "已将当前表格中的端口与波特率写回各设备 JSON 文件。")

    def closeEvent(self, event) -> None:
        self._close_device_csv_files()
        for row in self._device_rows:
            if row.get("serial") and row["serial"].is_open:
                try:
                    row["serial"].close()
                except Exception:
                    pass
        event.accept()


def main() -> None:
    app = QApplication(sys.argv)
    devices_json = os.path.join(SCRIPT_DIR, "config", "devices.json")
    w = UnifiedCollectorWindow(devices_json)
    w.show()
    sys.exit(app.exec())


if __name__ == "__main__":
    main()
