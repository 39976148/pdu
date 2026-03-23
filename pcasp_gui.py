#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
PCASP GUI 采集工具
- 串口选择（COM 列表刷新）
- PCASP 泵开关（通过发送 1B01 初始化 various settings bit1）
- 原始量显示（AD/计数）+ AD->物理量换算显示（V/°C/std cm^3/s）
- 30 bins 直方图实时显示（bin_1=0.1μm）
- CSV 自动保存（pcasp_data/pcasp_gui_YYYYMMDD_HHMMSS.csv）
"""

from __future__ import annotations

import csv
import os
import queue
import threading
import time
from datetime import datetime

import serial
from serial.tools import list_ports

import tkinter as tk
from tkinter import ttk, messagebox

import matplotlib
matplotlib.use("TkAgg")
from matplotlib.backends.backend_tkagg import FigureCanvasTkAgg
from matplotlib.figure import Figure

from pcasp_receiver import (
    build_get_data_cmd,
    build_init_cmd,
    read_exact,
    parse_frame,
    ad_to_physical,
)


EXPECTED_LEN_TO_BINS = {74: 10, 114: 20, 154: 30, 194: 40}
MAX_FRAME_LEN = max(EXPECTED_LEN_TO_BINS.keys())

BIN_SIZES_UM_30 = [
    0.10, 0.11, 0.12, 0.13, 0.14, 0.15, 0.16, 0.17, 0.18, 0.20,
    0.22, 0.24, 0.26, 0.28, 0.30, 0.40, 0.50, 0.60, 0.80, 1.00,
    1.20, 1.40, 1.60, 1.80, 2.00, 2.20, 2.40, 2.60, 2.80, 3.00,
]


def list_com_ports() -> list[str]:
    ports = []
    for p in list_ports.comports():
        ports.append(p.device)
    return ports


class PcaspWorker(threading.Thread):
    def __init__(
        self,
        out_q: queue.Queue,
        stop_evt: threading.Event,
        port: str,
        baud: int,
        desired_bins: int,
        interval_s: float,
        timeout_s: float,
        adc_threshold: int,
        pump_on: bool,
        autosave_csv: bool,
    ):
        super().__init__(daemon=True)
        self.out_q = out_q
        self.stop_evt = stop_evt
        self.port = port
        self.baud = baud
        self.desired_bins = desired_bins
        self.interval_s = interval_s
        self.timeout_s = timeout_s
        self.adc_threshold = adc_threshold
        self.pump_on = pump_on
        self.autosave_csv = autosave_csv

        self.ser: serial.Serial | None = None
        self.csv_fh = None
        self.writer = None
        self.csv_path = None

    def _emit(self, kind: str, payload: dict):
        self.out_q.put({"kind": kind, "payload": payload})

    def _open_csv(self, bins: int):
        if not self.autosave_csv or self.writer is not None:
            return
        data_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "pcasp_data")
        os.makedirs(data_dir, exist_ok=True)
        self.csv_path = os.path.join(data_dir, f"pcasp_gui_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv")
        self.csv_fh = open(self.csv_path, "w", encoding="utf-8-sig", newline="")
        self.writer = csv.writer(self.csv_fh)
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
        ] + [f"bin_{i+1}" for i in range(bins)]
        self.writer.writerow(headers)
        self.csv_fh.flush()
        self._emit("csv", {"path": self.csv_path})

    def _write_csv_row(self, ts: str, d: dict):
        if not self.writer:
            return
        row = [
            ts,
            d.get("hi_gain_baseline"), d.get("hi_gain_baseline_scaled"),
            d.get("mid_gain_baseline"), d.get("mid_gain_baseline_scaled"),
            d.get("low_gain_baseline"), d.get("low_gain_baseline_scaled"),
            d.get("sample_flow"), d.get("sample_flow_scaled"),
            d.get("laser_ref_voltage"), d.get("laser_ref_voltage_scaled"),
            d.get("aux_analog_1"), d.get("aux_analog_1_scaled"),
            d.get("sheath_flow"), d.get("sheath_flow_scaled"),
            d.get("electronics_temp"), d.get("electronics_temp_scaled"),
            d.get("avg_transit"),
            d.get("fifo_full"),
            d.get("reset_flag"),
            d.get("sync_err_a"),
            d.get("sync_err_b"),
            d.get("sync_err_c"),
            d.get("adc_overflow"),
        ] + list(d.get("bin_counts") or [])
        row = ["" if isinstance(x, float) and (x != x) else x for x in row]  # NaN -> ""
        self.writer.writerow(row)
        self.csv_fh.flush()

    def run(self):
        try:
            self.ser = serial.Serial(
                port=self.port,
                baudrate=self.baud,
                bytesize=serial.EIGHTBITS,
                parity=serial.PARITY_NONE,
                stopbits=serial.STOPBITS_ONE,
                timeout=0.1,
            )
            try:
                self.ser.dtr = True
                self.ser.rts = False
            except Exception:
                pass
        except Exception as e:
            self._emit("error", {"message": f"打开串口失败：{e}"})
            return

        # 初始化：强制 desired_bins + pump_on + threshold
        try:
            init_cmd = build_init_cmd(self.desired_bins, adc_threshold=self.adc_threshold, pump_on=self.pump_on)
            self.ser.reset_input_buffer()
            self.ser.write(init_cmd)
            self.ser.flush()
            time.sleep(0.5)
            r = self.ser.read(128)
            ok = len(r) >= 2 and r[:2] == bytes([0x06, 0x06])
            self._emit("status", {"message": f"init: {'0606 OK' if ok else '未收到0606'} (bins={self.desired_bins} thr={self.adc_threshold} pump={'ON' if self.pump_on else 'OFF'})"})
        except Exception as e:
            self._emit("error", {"message": f"发送 init 失败：{e}"})

        cmd = build_get_data_cmd()
        detected_bins = None

        while not self.stop_evt.is_set():
            try:
                self.ser.reset_input_buffer()
                self.ser.write(cmd)
                time.sleep(0.05)
                raw = read_exact(self.ser, MAX_FRAME_LEN, timeout_s=self.timeout_s)
                if len(raw) not in EXPECTED_LEN_TO_BINS:
                    self._emit("status", {"message": f"读帧长度异常: {len(raw)} (期望 74/114/154/194)"})
                    time.sleep(self.interval_s)
                    continue
                bins = EXPECTED_LEN_TO_BINS[len(raw)]
                if detected_bins != bins:
                    detected_bins = bins
                    self._emit("status", {"message": f"自动识别 bins={bins} reply_len={len(raw)}"})
                    self._open_csv(bins)

                d = parse_frame(raw, bins)
                if not d or not d.get("_checksum_ok"):
                    self._emit("status", {"message": "checksum/解析失败"})
                    time.sleep(self.interval_s)
                    continue
                ad_to_physical(d)
                ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                self._write_csv_row(ts, d)
                self._emit("data", {"ts": ts, "bins": bins, "frame": d})
            except Exception as e:
                self._emit("error", {"message": f"采集异常：{e}"})
                time.sleep(0.5)
            time.sleep(max(0.05, self.interval_s))

        try:
            if self.csv_fh:
                self.csv_fh.close()
        finally:
            try:
                if self.ser:
                    self.ser.close()
            except Exception:
                pass
        self._emit("status", {"message": "已停止"})


class PcaspGui(tk.Tk):
    def __init__(self):
        super().__init__()
        self.title("PCASP GUI 采集工具")
        self.geometry("1200x720")

        self.out_q: queue.Queue = queue.Queue()
        self.stop_evt = threading.Event()
        self.worker: PcaspWorker | None = None

        self.port_var = tk.StringVar(value="COM11")
        self.baud_var = tk.IntVar(value=38400)
        self.bins_var = tk.IntVar(value=30)
        self.thr_var = tk.IntVar(value=90)
        self.pump_var = tk.BooleanVar(value=True)
        self.interval_var = tk.DoubleVar(value=1.0)
        self.timeout_var = tk.DoubleVar(value=1.5)
        self.autosave_var = tk.BooleanVar(value=True)

        self.status_var = tk.StringVar(value="就绪")
        self.csv_var = tk.StringVar(value="")

        self._build_ui()
        self._build_plot()
        self.after(100, self._poll_queue)
        self._refresh_ports()

    def _build_ui(self):
        top = ttk.Frame(self)
        top.pack(side=tk.TOP, fill=tk.X, padx=10, pady=8)

        ttk.Label(top, text="串口").grid(row=0, column=0, sticky="w")
        self.port_cb = ttk.Combobox(top, textvariable=self.port_var, width=12, values=[])
        self.port_cb.grid(row=0, column=1, padx=6)
        ttk.Button(top, text="刷新", command=self._refresh_ports).grid(row=0, column=2, padx=6)

        ttk.Label(top, text="波特率").grid(row=0, column=3, sticky="w")
        ttk.Entry(top, textvariable=self.baud_var, width=8).grid(row=0, column=4, padx=6)

        ttk.Label(top, text="bins").grid(row=0, column=5, sticky="w")
        ttk.Entry(top, textvariable=self.bins_var, width=5).grid(row=0, column=6, padx=6)

        ttk.Label(top, text="Threshold").grid(row=0, column=7, sticky="w")
        ttk.Entry(top, textvariable=self.thr_var, width=6).grid(row=0, column=8, padx=6)

        ttk.Checkbutton(top, text="泵 ON", variable=self.pump_var).grid(row=0, column=9, padx=10)
        ttk.Checkbutton(top, text="CSV自动保存", variable=self.autosave_var).grid(row=0, column=10, padx=10)

        ttk.Label(top, text="interval(s)").grid(row=1, column=0, sticky="w", pady=(8, 0))
        ttk.Entry(top, textvariable=self.interval_var, width=6).grid(row=1, column=1, padx=6, pady=(8, 0))
        ttk.Label(top, text="timeout(s)").grid(row=1, column=3, sticky="w", pady=(8, 0))
        ttk.Entry(top, textvariable=self.timeout_var, width=6).grid(row=1, column=4, padx=6, pady=(8, 0))

        ttk.Button(top, text="开始采集", command=self.start).grid(row=1, column=7, padx=6, pady=(8, 0))
        ttk.Button(top, text="停止", command=self.stop).grid(row=1, column=8, padx=6, pady=(8, 0))
        ttk.Button(top, text="发送泵/参数(init)", command=self.send_init_once).grid(row=1, column=9, padx=6, pady=(8, 0))

        status = ttk.Frame(self)
        status.pack(side=tk.TOP, fill=tk.X, padx=10, pady=(0, 8))
        ttk.Label(status, textvariable=self.status_var).pack(side=tk.LEFT)
        ttk.Label(status, textvariable=self.csv_var).pack(side=tk.RIGHT)

        mid = ttk.Frame(self)
        mid.pack(side=tk.TOP, fill=tk.X, padx=10, pady=6)

        self.fields = {}
        # 原始 + 换算显示
        labels = [
            ("sample_flow", "Sample Flow AD", "sample_flow_scaled", "std cm³/s"),
            ("sheath_flow", "Sheath Flow AD", "sheath_flow_scaled", "std cm³/s"),
            ("electronics_temp", "Electronics Temp AD", "electronics_temp_scaled", "°C"),
            ("laser_ref_voltage", "Laser Ref AD", "laser_ref_voltage_scaled", "V"),
            ("aux_analog_1", "Aux Analog1 AD", "aux_analog_1_scaled", "V"),
            ("hi_gain_baseline", "Hi Gain AD", "hi_gain_baseline_scaled", "V"),
            ("mid_gain_baseline", "Mid Gain AD", "mid_gain_baseline_scaled", "V"),
            ("low_gain_baseline", "Low Gain AD", "low_gain_baseline_scaled", "V"),
        ]
        for i, (raw_k, raw_name, scaled_k, unit) in enumerate(labels):
            frm = ttk.LabelFrame(mid, text=raw_name)
            frm.grid(row=i // 4, column=i % 4, padx=6, pady=6, sticky="ew")
            raw_v = tk.StringVar(value="—")
            scaled_v = tk.StringVar(value="—")
            ttk.Label(frm, text="raw").grid(row=0, column=0, sticky="w")
            ttk.Label(frm, textvariable=raw_v, width=12).grid(row=0, column=1, sticky="w")
            ttk.Label(frm, text="scaled").grid(row=1, column=0, sticky="w")
            ttk.Label(frm, textvariable=scaled_v, width=12).grid(row=1, column=1, sticky="w")
            ttk.Label(frm, text=unit).grid(row=1, column=2, sticky="w")
            self.fields[raw_k] = raw_v
            self.fields[scaled_k] = scaled_v

        self.bin1_var = tk.StringVar(value="—")
        binfrm = ttk.LabelFrame(mid, text="bin_1 (0.1μm)")
        binfrm.grid(row=0, column=4, rowspan=2, padx=6, pady=6, sticky="nsew")
        ttk.Label(binfrm, textvariable=self.bin1_var, font=("Segoe UI", 18, "bold")).pack(padx=12, pady=18)

    def _build_plot(self):
        bottom = ttk.Frame(self)
        bottom.pack(side=tk.TOP, fill=tk.BOTH, expand=True, padx=10, pady=8)

        fig = Figure(figsize=(10, 4), dpi=100)
        ax = fig.add_subplot(111)
        ax.set_title("Histogram (bin counts)")
        ax.set_xlabel("Size (μm)")
        ax.set_ylabel("Counts")
        ax.set_xlim(-0.05, 3.2)
        self.fig = fig
        self.ax = ax
        self.bars = ax.bar(BIN_SIZES_UM_30, [0] * 30, width=0.008, color="steelblue", edgecolor="navy", alpha=0.85)
        self.canvas = FigureCanvasTkAgg(fig, master=bottom)
        self.canvas.get_tk_widget().pack(fill=tk.BOTH, expand=True)
        self.canvas.draw()

    def _refresh_ports(self):
        ports = list_com_ports()
        self.port_cb["values"] = ports
        if ports and self.port_var.get() not in ports:
            self.port_var.set(ports[0])

    def _poll_queue(self):
        try:
            while True:
                msg = self.out_q.get_nowait()
                kind = msg.get("kind")
                payload = msg.get("payload") or {}
                if kind == "status":
                    self.status_var.set(payload.get("message", ""))
                elif kind == "csv":
                    self.csv_var.set(f"CSV: {payload.get('path')}")
                elif kind == "error":
                    self.status_var.set(payload.get("message", "错误"))
                elif kind == "data":
                    self._on_data(payload)
        except queue.Empty:
            pass
        self.after(100, self._poll_queue)

    def _fmt(self, v, nd=3):
        if v is None:
            return "—"
        if isinstance(v, float) and v != v:
            return "—"
        if isinstance(v, float):
            return f"{v:.{nd}f}"
        return str(v)

    def _on_data(self, payload: dict):
        frame = payload.get("frame") or {}
        # 更新字段
        for k, var in self.fields.items():
            var.set(self._fmt(frame.get(k)))
        # bin1
        bins = frame.get("bin_counts") or []
        self.bin1_var.set(str(bins[0] if bins else "—"))
        # histogram（仅当 30 bins 时展示 30 个）
        if payload.get("bins") == 30 and len(bins) >= 30:
            for i, b in enumerate(self.bars):
                b.set_height(bins[i])
            self.ax.relim()
            self.ax.autoscale_view(scalex=False)
            self.canvas.draw_idle()

    def start(self):
        if self.worker and self.worker.is_alive():
            messagebox.showinfo("提示", "正在采集中")
            return
        port = self.port_var.get().strip()
        if not port:
            messagebox.showerror("错误", "请选择串口")
            return
        self.stop_evt.clear()
        self.csv_var.set("")
        self.worker = PcaspWorker(
            out_q=self.out_q,
            stop_evt=self.stop_evt,
            port=port,
            baud=int(self.baud_var.get()),
            desired_bins=int(self.bins_var.get()),
            interval_s=float(self.interval_var.get()),
            timeout_s=float(self.timeout_var.get()),
            adc_threshold=int(self.thr_var.get()),
            pump_on=bool(self.pump_var.get()),
            autosave_csv=bool(self.autosave_var.get()),
        )
        self.status_var.set("启动中...")
        self.worker.start()

    def stop(self):
        self.stop_evt.set()

    def send_init_once(self):
        # 单次发送 init（不启动采集线程）用于切换泵/通道/阈值
        port = self.port_var.get().strip()
        if not port:
            messagebox.showerror("错误", "请选择串口")
            return
        try:
            ser = serial.Serial(port=port, baudrate=int(self.baud_var.get()), timeout=0.2)
            try:
                ser.dtr = True
                ser.rts = False
            except Exception:
                pass
            cmd = build_init_cmd(
                int(self.bins_var.get()),
                adc_threshold=int(self.thr_var.get()),
                pump_on=bool(self.pump_var.get()),
            )
            ser.reset_input_buffer()
            ser.write(cmd)
            ser.flush()
            time.sleep(0.5)
            r = ser.read(64)
            ok = len(r) >= 2 and r[:2] == bytes([0x06, 0x06])
            self.status_var.set(f"手动 init: {'0606 OK' if ok else '未收到0606'}")
        except Exception as e:
            messagebox.showerror("错误", f"发送 init 失败：{e}")
        finally:
            try:
                ser.close()
            except Exception:
                pass


def main():
    app = PcaspGui()
    app.mainloop()


if __name__ == "__main__":
    main()

