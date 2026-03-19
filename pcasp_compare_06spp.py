#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
对比 06SPP CSV 与 Python 采集的每 bin 数量级。
- 06SPP: SPP_200_OPC_ch0..ch29，典型为几十到几百（与 06SPP_20020260317195342.csv 一致）。
- Python 当前按 30×U32 解析：每 bin 4 字节，若设备发的是 16 位计数则我们会读到错误数量级。
- 若设备在 32 字节头后发 30×U16（60 字节）再 60 字节填充，则应用 30×U16 解析以与 06SPP 一致。
"""

import csv
import os

# 06SPP CSV：前 19 行为描述，第 20 行为表头，第 21 行起为数据
CSV_PATH = os.path.join(os.path.dirname(__file__), "06SPP_20020260317195342.csv")
CH_COLS = [f"SPP_200_OPC_ch{i}" for i in range(30)]


def main():
    if not os.path.isfile(CSV_PATH):
        print(f"未找到: {CSV_PATH}")
        return
    with open(CSV_PATH, "r", encoding="utf-8-sig") as f:
        for _ in range(19):
            f.readline()
        reader = csv.DictReader(f)
        headers = reader.fieldnames
        if not headers:
            print("无表头")
            return
        # 取前几行数据
        rows = []
        for i, row in enumerate(reader):
            if i >= 5:
                break
            rows.append(row)

    print("06SPP CSV 前几行 ch0-ch29（与 bin_1-bin_30 对应）：")
    print("列名:", CH_COLS[:5], "...", CH_COLS[-2:])
    for i, row in enumerate(rows):
        vals = [int(row.get(c, 0)) for c in CH_COLS]
        print(f"  行{i+1}: {vals[:8]} ... {vals[-4:]}")
    if rows:
        v0 = [int(rows[0].get(c, 0)) for c in CH_COLS]
        print(f"\n数量级: min={min(v0)}, max={max(v0)}, 首 bin(ch0)={v0[0]}")
    print("\n说明: Python 若按 30×U32 解析且设备实际发 30×U16，会得到错误数量级；")
    print("      已为 pcasp_receiver 增加 30×U16 直方图解析选项，与 06SPP 一致。")


if __name__ == "__main__":
    main()
