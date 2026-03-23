#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
PCASP-100X 协议工具：
- build_init_cmd: 1B01 初始化命令
- build_get_data_cmd: 1B02 取数命令
- read_exact: 指定长度读取
- parse_frame: 解析 74/114/154/194 字节回复
- ad_to_physical: AD -> 物理量换算
"""

from __future__ import annotations

import math
import struct
import time
from typing import Optional


SCALE_LINEAR = "linear"
SCALE_POLY4 = "poly4"
SCALE_THERMISTOR_D = "thermistor_d"

# (equation_type, A, B, C, D, E)
SCALING = [
    (SCALE_LINEAR, -9.999995, 0.004883, 0.0, 0.0, 0.0),
    (SCALE_LINEAR, -9.999995, 0.004883, 0.0, 0.0, 0.0),
    (SCALE_LINEAR, -9.999995, 0.004883, 0.0, 0.0, 0.0),
    (SCALE_POLY4, 13.1875, -0.012852, 3.12476e-6, 0.0, 0.0),
    (SCALE_LINEAR, -10.0, 0.004883, 0.0, 0.0, 0.0),
    (SCALE_LINEAR, -10.0, 0.004883, 0.0, 0.0, 0.0),
    (SCALE_POLY4, 92.512001, -0.08181, 1.81782e-5, 0.0, 0.0),
    (SCALE_THERMISTOR_D, -2273.399902, 0.97656, 0.0, 0.0, 0.0),
]
SCALE_KEYS = [
    "hi_gain_baseline",
    "mid_gain_baseline",
    "low_gain_baseline",
    "sample_flow",
    "laser_ref_voltage",
    "aux_analog_1",
    "sheath_flow",
    "electronics_temp",
]

DEFAULT_ADC_THRESHOLD = 90
DEFAULT_AVG_TRANS_SAMPLES = 6

EXAMPLE_THRESHOLDS_30 = [
    692, 1146, 1814, 2769, 4096, 4192, 4231, 4282, 4348, 4537,
    4825, 5251, 5859, 6703, 8192, 8335, 8435, 8520, 8767, 8981,
    9194, 9412, 9572, 9825, 10080, 10460, 10872, 11322, 11759, 12288,
]


def checksum_u16_le(payload: bytes) -> bytes:
    return struct.pack("<H", sum(payload) & 0xFFFF)


def build_init_cmd(
    bins: int,
    adc_threshold: Optional[int] = None,
    pump_on: bool = True,
    clock_divisor: bool = False,
) -> bytes:
    if bins not in (10, 20, 30, 40):
        raise ValueError("bins must be 10/20/30/40")
    cmd = bytearray([0x1B, 0x01])
    if adc_threshold is None:
        adc_threshold = DEFAULT_ADC_THRESHOLD
    cmd.extend(struct.pack("<H", int(adc_threshold)))  # ADC Threshold
    cmd.extend(struct.pack("<H", bins))  # Channel Count
    cmd.extend(struct.pack("<H", 0))  # Range
    cmd.extend(struct.pack("<H", DEFAULT_AVG_TRANS_SAMPLES))  # Avg Trans
    various = 0
    if clock_divisor:
        various |= 0x0001
    if pump_on:
        various |= 0x0002
    cmd.extend(struct.pack("<H", various))
    th = (EXAMPLE_THRESHOLDS_30[:bins] + [0] * 40)[:40]
    for v in th:
        cmd.extend(struct.pack("<H", v))
    cmd.extend(checksum_u16_le(cmd))
    return bytes(cmd)


def build_get_data_cmd() -> bytes:
    base = bytes([0x1B, 0x02])
    return base + checksum_u16_le(base)


def read_exact(ser, n: int, timeout_s: float) -> bytes:
    deadline = time.time() + timeout_s
    buf = bytearray()
    while len(buf) < n and time.time() < deadline:
        need = n - len(buf)
        chunk = ser.read(need)
        if chunk:
            buf.extend(chunk)
        else:
            time.sleep(0.002)
    return bytes(buf)


def parse_frame(raw: bytes, bins: int, swap_hist_words: bool = True) -> dict:
    exp_len = 32 + 4 * bins + 2
    if len(raw) != exp_len:
        return {}
    payload, cs_bytes = raw[:-2], raw[-2:]
    cs_calc = sum(payload) & 0xFFFF
    cs_recv = struct.unpack("<H", cs_bytes)[0]
    if cs_calc != cs_recv:
        return {"_checksum_ok": False, "_checksum_calc": cs_calc, "_checksum_recv": cs_recv}

    d = {"_checksum_ok": True}
    d["hi_gain_baseline"] = struct.unpack_from("<H", payload, 0)[0]
    d["mid_gain_baseline"] = struct.unpack_from("<H", payload, 2)[0]
    d["low_gain_baseline"] = struct.unpack_from("<H", payload, 4)[0]
    d["sample_flow"] = struct.unpack_from("<H", payload, 6)[0]
    d["laser_ref_voltage"] = struct.unpack_from("<H", payload, 8)[0]
    d["aux_analog_1"] = struct.unpack_from("<H", payload, 10)[0]
    d["sheath_flow"] = struct.unpack_from("<H", payload, 12)[0]
    d["electronics_temp"] = struct.unpack_from("<H", payload, 14)[0]
    d["avg_transit"] = struct.unpack_from("<H", payload, 16)[0]
    d["fifo_full"] = struct.unpack_from("<H", payload, 18)[0]
    d["reset_flag"] = struct.unpack_from("<H", payload, 20)[0]
    d["sync_err_a"] = struct.unpack_from("<H", payload, 22)[0]
    d["sync_err_b"] = struct.unpack_from("<H", payload, 24)[0]
    d["sync_err_c"] = struct.unpack_from("<H", payload, 26)[0]
    d["adc_overflow"] = struct.unpack_from("<I", payload, 28)[0]

    bins_list = []
    off = 32
    for i in range(bins):
        u32_raw = struct.unpack_from("<I", payload, off + i * 4)[0]
        if swap_hist_words:
            u32 = (u32_raw >> 16) | ((u32_raw & 0xFFFF) << 16)
        else:
            u32 = u32_raw
        bins_list.append(u32)
    d["bin_counts"] = bins_list
    return d


def _scale_linear(adc: float, a: float, b: float, *_cde) -> float:
    return a + b * adc


def _scale_poly4(adc: float, a: float, b: float, c: float, d: float, _e=0.0) -> float:
    return a + b * adc + c * (adc ** 2) + d * (adc ** 3)


def _scale_thermistor_d(adc: float, _a, _b, *_cde) -> float:
    if adc <= 0:
        return float("nan")
    x = 5.0 / (adc * 5.0 / 4096.0) - 1.0
    if x <= 0:
        return float("nan")
    inv_t = math.log(x) * (1.0 / 3750.0) + (1.0 / 298.0)
    if inv_t <= 0:
        return float("nan")
    return 1.0 / inv_t - 273.0


def ad_to_physical(d: dict) -> None:
    for i, key in enumerate(SCALE_KEYS):
        if key not in d or i >= len(SCALING):
            continue
        eq, a, b, c, d_val, e = SCALING[i]
        adc = float(d[key])
        if eq == SCALE_LINEAR:
            val = _scale_linear(adc, a, b, c, d_val, e)
        elif eq == SCALE_POLY4:
            val = _scale_poly4(adc, a, b, c, d_val, e)
        elif eq == SCALE_THERMISTOR_D:
            val = _scale_thermistor_d(adc, a, b, c, d_val, e)
        else:
            val = float("nan")
        d[key + "_scaled"] = val

