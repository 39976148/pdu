#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
PCASP-100X 串口接收程序（真实探头）

参考：03-pcasp-100x-introduction.txt
- 初始化命令：1B01 ... + checksum（可选，部分设备已在工作参数下可直接取数）
- 取数命令：1B02 + checksum（checksum = 所有前置字节之和取低16位，小端）
- 直方图按文档 Byte Swapped：每 bin 为 U32，设备发高16位在前，交换两字后为 Decimal 计数（与 03 文档 Bin 1=1, Bin 2=24 一致）。
- bin_1..bin_30 对应 SPP_200_OPC_ch0..ch29（bin_1=0.1μm）。Number_Conc 由它软件换算。

默认：
- 端口 COM11（RS-422 转 USB 后分配的 COM 口，协议与 RS-232 相同）
- 波特率 38400（可 --baud 115200 等）
- bins=30（对应回复 154 字节：32 + 4*bins + 2）
"""

import argparse
import math
import os
import struct
import time
from datetime import datetime

import serial

# SPCASP 数据结构示例.xlsx Scaling Coefficients：AD 计数 -> 物理量
# Linear: Data = A + B*ADC；4th Order Poly: A+B*ADC+C*ADC^2+D*ADC^3+E*ADC^4
# Thermistor D: T_C = 1/(ln(5/(ADC*5/4096)-1)*(1/3750)+(1/298)) - 273
SCALE_LINEAR = "linear"
SCALE_POLY4 = "poly4"
SCALE_THERMISTOR_D = "thermistor_d"

# (equation_type, A, B, C, D, E) 与帧字段顺序对应
SCALING = [
    (SCALE_LINEAR, -9.999995, 0.004883, 0.0, 0.0, 0.0),   # Hi Gain Baseline -> V
    (SCALE_LINEAR, -9.999995, 0.004883, 0.0, 0.0, 0.0),   # Mid Gain Baseline -> V
    (SCALE_LINEAR, -9.999995, 0.004883, 0.0, 0.0, 0.0),   # Low Gain Baseline -> V
    (SCALE_POLY4, 13.1875, -0.012852, 3.12476e-6, 0.0, 0.0),   # Sample Flow -> std cm^3/s
    (SCALE_LINEAR, -10.0, 0.004883, 0.0, 0.0, 0.0),       # Laser Ref Voltage -> V
    (SCALE_LINEAR, -10.0, 0.004883, 0.0, 0.0, 0.0),       # Aux Analog 1 -> V
    (SCALE_POLY4, 92.512001, -0.08181, 1.81782e-5, 0.0, 0.0),  # Sheath Flow -> std cm^3/s
    (SCALE_THERMISTOR_D, -2273.399902, 0.97656, 0.0, 0.0, 0.0),  # Electronics Temp -> °C
]
SCALE_KEYS = [
    "hi_gain_baseline", "mid_gain_baseline", "low_gain_baseline",
    "sample_flow", "laser_ref_voltage", "aux_analog_1", "sheath_flow", "electronics_temp",
]

# 参考 06SPP_200*.csv 头部：Threshold=20（更接近现场配置）；该值会显著影响粒子计数触发
DEFAULT_ADC_THRESHOLD = 20


def checksum_u16_le(payload: bytes) -> bytes:
    return struct.pack("<H", sum(payload) & 0xFFFF)


def build_init_cmd(bins: int, adc_threshold: int | None = None, pump_on: bool = True, clock_divisor: bool = False) -> bytes:
    """
    发送 1B01 初始化（参考文档示例，均为小端 U16）。
    默认参数：ADC=20（参考 06SPP 头部），Range=0，AvgTrans=128，Pump=TRUE；阈值用示例值并补 0 到 40 bins。
    """
    if bins not in (10, 20, 30, 40):
        raise ValueError("bins must be 10/20/30/40")
    cmd = bytearray([0x1B, 0x01])
    if adc_threshold is None:
        adc_threshold = DEFAULT_ADC_THRESHOLD
    cmd.extend(struct.pack("<H", int(adc_threshold)))          # ADC Threshold
    cmd.extend(struct.pack("<H", bins))        # Channel Count
    cmd.extend(struct.pack("<H", 0))           # Range
    cmd.extend(struct.pack("<H", 128))         # Avg Trans Sample Numbers
    various = 0
    if clock_divisor:
        various |= 0x0001
    if pump_on:
        various |= 0x0002
    cmd.extend(struct.pack("<H", various))           # various settings
    example_th = [
        692, 1146, 1814, 2769, 4096, 4192, 4231, 4282, 4348, 4537,
        4825, 5251, 5859, 6703, 8192, 8335, 8435, 8520, 8767, 8981,
        9194, 9412, 9572, 9825, 10080, 10460, 10872, 11322, 11759, 12288,
    ]
    th = (example_th[:bins] + [0] * 40)[:40]
    for v in th:
        cmd.extend(struct.pack("<H", v))
    cmd.extend(checksum_u16_le(cmd))
    return bytes(cmd)


def build_get_data_cmd() -> bytes:
    base = bytes([0x1B, 0x02])
    return base + checksum_u16_le(base)


def read_exact(ser: serial.Serial, n: int, timeout_s: float) -> bytes:
    """读取恰好 n 字节（允许多次 read），超时返回已读到的字节。"""
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


def parse_frame(raw: bytes, bins: int) -> dict:
    """解析一帧数据（payload+checksum）。
    文档 03-pcasp-100x-introduction.txt：Histogram Data 为 Raw Data，需 Byte Swapped 得 Decimal。
    即每 bin 为 32 位，设备发【高16位在前、低16位在后】，交换两字后为真实计数。
    """
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
        # Byte Swapped（文档）：交换高16位与低16位得 Decimal Data
        u32 = (u32_raw >> 16) | ((u32_raw & 0xFFFF) << 16)
        bins_list.append(u32)
    d["bin_counts"] = bins_list
    return d


def _scale_linear(adc: float, a: float, b: float, *_cde) -> float:
    return a + b * adc


def _scale_poly4(adc: float, a: float, b: float, c: float, d: float, _e=0.0) -> float:
    return a + b * adc + c * (adc ** 2) + d * (adc ** 3)


def _scale_thermistor_d(adc: float, _a, _b, *_cde) -> float:
    """Electronics Temp: T_C = 1/(ln(5/(ADC*5/4096)-1)*(1/3750)+(1/298)) - 273"""
    if adc <= 0:
        return float("nan")
    x = 5.0 / (adc * 5.0 / 4096.0) - 1.0
    if x <= 0:
        return float("nan")
    ln_x = math.log(x)
    inv_t = ln_x * (1.0 / 3750.0) + (1.0 / 298.0)
    if inv_t <= 0:
        return float("nan")
    return 1.0 / inv_t - 273.0


def ad_to_physical(d: dict) -> None:
    """根据 SPCASP Scaling Coefficients 将 AD 计数字段换算为物理量，结果写入 d 的 *_scaled 键。"""
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


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--port", default="COM11", help="串口（RS-422 转 COM 后，如 COM11）")
    ap.add_argument("--baud", type=int, default=38400)
    ap.add_argument("--bins", type=int, default=30, choices=[10, 20, 30, 40])
    ap.add_argument("--interval", type=float, default=1.0, help="取数间隔（秒）")
    ap.add_argument("--timeout", type=float, default=1.5, help="单次读帧超时（秒）")
    ap.add_argument("--init", action="store_true", help="启动时先发送 1B01 初始化")
    ap.add_argument("--no-csv", action="store_true", help="不保存 CSV，仅打印")
    ap.add_argument("--debug", action="store_true", help="调试：打印收发原始字节(hex)")
    args = ap.parse_args()

    expected_len_to_bins = {74: 10, 114: 20, 154: 30, 194: 40}
    max_exp_len = max(expected_len_to_bins.keys())
    exp_len = 32 + 4 * args.bins + 2
    print(
        f"PCASP-100X Receiver  port={args.port} baud={args.baud} bins={args.bins} reply_len={exp_len} "
        f"(直方图 Byte Swapped; auto-detect 74/114/154/194)"
    )

    try:
        ser = serial.Serial(
            port=args.port,
            baudrate=args.baud,
            bytesize=serial.EIGHTBITS,
            parity=serial.PARITY_NONE,
            stopbits=serial.STOPBITS_ONE,
            timeout=0.2,
        )
    except Exception as e:
        print(f"打开串口失败：{e}")
        return

    try:
        ser.dtr = True
        ser.rts = False
        time.sleep(0.1)
    except Exception:
        pass

    if args.debug:
        print(" [DEBUG] 监听 2 秒，看是否有上电/自发数据...")
        ser.reset_input_buffer()
        time.sleep(2.0)
        any_in = ser.read(256)
        if any_in:
            print(f" [DEBUG] 收到 {len(any_in)} 字节: {any_in.hex()}")
        else:
            print(" [DEBUG] 2 秒内无数据")
        print(" [DEBUG] 若始终 0 字节：请确认 COM10 是否接 PCASP、TX/RX 是否接对、探头是否上电；可试 --baud 9600/38400/57600")

    csv_fh = None
    writer = None
    csv = None
    csv_path = None
    detected_bins = None
    if not args.no_csv:
        import csv as _csv
        csv = _csv

    cmd = build_get_data_cmd()
    try:
        if args.init:
            init_cmd = build_init_cmd(args.bins)
            ser.reset_input_buffer()
            ser.write(init_cmd)
            ser.flush()
            if args.debug:
                print(f" [DEBUG] 已发 init {len(init_cmd)} 字节: {init_cmd[:20].hex()}...")
            time.sleep(0.5)
            r = ser.read(128)
            if r == bytes([0x06, 0x06]) or (len(r) >= 2 and r[:2] == bytes([0x06, 0x06])):
                print("[OK] init: 收到 0606")
                if args.debug and len(r) > 2:
                    print(f" [DEBUG] init 额外字节: {r[2:].hex()}")
            else:
                print(f"[WARN] init: 未收到 0606（收到 {len(r)} 字节: {r.hex() if r else '空'}），继续尝试取数")
        while True:
            ser.reset_input_buffer()
            ser.write(cmd)
            if args.debug:
                print(f" [DEBUG] 已发 get_data: {cmd.hex()}")
            time.sleep(0.05)
            raw = read_exact(ser, max_exp_len, timeout_s=args.timeout)
            if args.debug and raw:
                print(f" [DEBUG] 收到 {len(raw)} 字节，前32字节: {raw[:32].hex()}")
            if len(raw) in expected_len_to_bins and detected_bins != expected_len_to_bins[len(raw)]:
                detected_bins = expected_len_to_bins[len(raw)]
                exp_len = len(raw)
                print(f"[OK] 自动识别 bins={detected_bins} reply_len={exp_len}")
                if writer is None and not args.no_csv:
                    data_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "pcasp_data")
                    os.makedirs(data_dir, exist_ok=True)
                    csv_path = os.path.join(data_dir, f"pcasp_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv")
                    csv_fh = open(csv_path, "w", encoding="utf-8-sig", newline="")
                    headers = [
                        "date_time",
                        "hi_gain_baseline", "hi_gain_baseline_V",
                        "mid_gain_baseline", "mid_gain_baseline_V",
                        "low_gain_baseline", "low_gain_baseline_V",
                        "sample_flow", "sample_flow_std_cm3_s",
                        "laser_ref_voltage", "laser_ref_voltage_V",
                        "aux_analog_1", "aux_analog_1_V",
                        "sheath_flow", "sheath_flow_std_cm3_s",
                        "electronics_temp", "electronics_temp_C",
                        "avg_transit",
                        "fifo_full",
                        "reset_flag",
                        "sync_err_a",
                        "sync_err_b",
                        "sync_err_c",
                        "adc_overflow",
                    ] + [f"bin_{i+1}" for i in range(detected_bins)]
                    writer = csv.writer(csv_fh)
                    writer.writerow(headers)
                    print(f"CSV: {csv_path} （AD+物理量换算，bin_1=0.1μm）")

            if len(raw) not in expected_len_to_bins:
                if raw and args.debug:
                    print(f" [DEBUG] 完整收包(hex): {raw.hex()}")
                print(f"[WARN] 读帧长度异常：{len(raw)}/{exp_len}（期望 74/114/154/194）")
            else:
                bins_to_parse = expected_len_to_bins[len(raw)]
                d = parse_frame(raw, bins_to_parse)
                if not d:
                    print("[WARN] 解析失败")
                elif not d.get("_checksum_ok", False):
                    print(f"[WARN] checksum 不匹配 calc={d.get('_checksum_calc')} recv={d.get('_checksum_recv')}")
                else:
                    ad_to_physical(d)
                    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                    sf = d.get("sample_flow_scaled")
                    tc = d.get("electronics_temp_scaled")
                    if sf is None or (isinstance(sf, float) and math.isnan(sf)):
                        sf = d["sample_flow"]
                    if tc is None or (isinstance(tc, float) and math.isnan(tc)):
                        tc = d["electronics_temp"]
                    sf_str = f"{sf:.4f}" if isinstance(sf, (int, float)) and not math.isnan(sf) else str(sf)
                    tc_str = f"{tc:.1f}" if isinstance(tc, (int, float)) and not math.isnan(tc) else str(tc)
                    print(
                        f"[OK] {ts} sample_flow={sf_str} std_cm3/s temp={tc_str}°C "
                        f"bin1={d['bin_counts'][0] if d['bin_counts'] else 0}"
                    )
                    if writer:
                        row = [
                            ts,
                            d["hi_gain_baseline"], d.get("hi_gain_baseline_scaled"),
                            d["mid_gain_baseline"], d.get("mid_gain_baseline_scaled"),
                            d["low_gain_baseline"], d.get("low_gain_baseline_scaled"),
                            d["sample_flow"], d.get("sample_flow_scaled"),
                            d["laser_ref_voltage"], d.get("laser_ref_voltage_scaled"),
                            d["aux_analog_1"], d.get("aux_analog_1_scaled"),
                            d["sheath_flow"], d.get("sheath_flow_scaled"),
                            d["electronics_temp"], d.get("electronics_temp_scaled"),
                            d["avg_transit"],
                            d["fifo_full"],
                            d["reset_flag"],
                            d["sync_err_a"],
                            d["sync_err_b"],
                            d["sync_err_c"],
                            d["adc_overflow"],
                        ] + d["bin_counts"]
                        row = ["" if isinstance(x, float) and math.isnan(x) else x for x in row]
                        writer.writerow(row)
                        csv_fh.flush()
            time.sleep(max(0.05, args.interval))
    except KeyboardInterrupt:
        print("已停止")
    finally:
        try:
            if csv_fh:
                csv_fh.close()
        finally:
            ser.close()


if __name__ == "__main__":
    main()

