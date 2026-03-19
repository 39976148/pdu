#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
PCASP-100X 采集并直方图显示（固定参数）
- COM11, 38400, 30 bins 写入程序，不通过命令行传参。
- 实时显示 bin_1..bin_30 的直方图，横轴为粒径(μm)，与参考 CSV Sizes 一致。
"""

import time
import serial
import matplotlib
matplotlib.use("QtAgg")
import matplotlib.pyplot as plt
import matplotlib as mpl
mpl.rcParams["font.sans-serif"] = ["SimHei", "Microsoft YaHei", "DejaVu Sans"]
mpl.rcParams["axes.unicode_minus"] = False

from pcasp_receiver import (
    build_init_cmd,
    build_get_data_cmd,
    read_exact,
    parse_frame,
)

# 固定参数（不通过命令行）
PORT = "COM11"
BAUDRATE = 38400
BINS = 30
EXP_LEN = 32 + 4 * BINS + 2  # 154
INIT_ON_START = True
INTERVAL_S = 1.0
READ_TIMEOUT_S = 1.5

# 30 通道对应粒径 (μm)，与参考 06SPP_200...csv 中 Sizes 一致
BIN_SIZES_UM = [
    0.10, 0.11, 0.12, 0.13, 0.14, 0.15, 0.16, 0.17, 0.18, 0.20,
    0.22, 0.24, 0.26, 0.28, 0.30, 0.40, 0.50, 0.60, 0.80, 1.00,
    1.20, 1.40, 1.60, 1.80, 2.00, 2.20, 2.40, 2.60, 2.80, 3.00,
]


def main():
    ser = serial.Serial(
        port=PORT,
        baudrate=BAUDRATE,
        bytesize=serial.EIGHTBITS,
        parity=serial.PARITY_NONE,
        stopbits=serial.STOPBITS_ONE,
        timeout=0.1,
    )
    ser.dtr = True
    ser.rts = False

    if INIT_ON_START:
        init_cmd = build_init_cmd(BINS)
        ser.reset_input_buffer()
        ser.write(init_cmd)
        ser.flush()
        time.sleep(0.5)
        r = ser.read(128)
        if r[:2] == bytes([0x06, 0x06]):
            print("[OK] init: 收到 0606")
        else:
            print(f"[WARN] init: 未收到 0606，继续取数")

    cmd = build_get_data_cmd()
    fig, ax = plt.subplots(figsize=(10, 5))
    x = BIN_SIZES_UM
    bars = ax.bar(x, [0] * BINS, width=0.008, color="steelblue", edgecolor="navy", alpha=0.85)
    ax.set_xlabel("粒径 (μm)")
    ax.set_ylabel("计数 (cts)")
    ax.set_title("PCASP-100X 粒径分布 (COM11, 38400, 30 bins)")
    ax.set_xlim(-0.05, 3.2)
    plt.tight_layout()
    plt.ion()
    plt.show()

    try:
        while True:
            ser.reset_input_buffer()
            ser.write(cmd)
            time.sleep(0.05)
            raw = read_exact(ser, EXP_LEN, timeout_s=READ_TIMEOUT_S)
            if len(raw) != EXP_LEN:
                time.sleep(INTERVAL_S)
                continue
            d = parse_frame(raw, BINS)
            if not d or not d.get("_checksum_ok"):
                time.sleep(INTERVAL_S)
                continue
            counts = d["bin_counts"]
            for i, b in enumerate(bars):
                b.set_height(counts[i] if i < len(counts) else 0)
            ax.relim()
            ax.autoscale_view(scalex=False)
            fig.canvas.draw_idle()
            fig.canvas.flush_events()
            plt.pause(0.02)
            time.sleep(max(0.05, INTERVAL_S - 0.1))
    except KeyboardInterrupt:
        print("已停止")
    finally:
        plt.ioff()
        plt.close()
        ser.close()


if __name__ == "__main__":
    main()
