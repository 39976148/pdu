# -*- coding: utf-8 -*-
"""
Pdu3 电流/功率/电能 实时数据测试 — 控制台循环读取 192.168.1.163 的 6 个插座电流、功率和电能。

用法: python pdu3_current_test.py
按 Ctrl+C 退出。

单位说明（来自 OID 表）：
- 电流：8.1.4.x，单位 0.01A（原始值 /100 = A）
- 功率：8.1.5.x，单位 1W（原始值 = W）
- 电能：8.1.7.x，单位 1Wh（原始值 = Wh，/1000 = kWh）
"""
import asyncio
import sys
import time
from datetime import datetime
from typing import List, Optional

PDU3_IP = "192.168.1.163"
SNMP_PORT = 161
SNMP_COMMUNITY = "public"
OUTLETS = 6
INTERVAL_SEC = 2

# DPDU V2V3-L：各插座电流/功率/电能 OID
# 电流：1.3.6.1.4.1.23280.8.1.4.1~6（outputCurrent1~6），单位 0.01A → /100 = A
# 功率：1.3.6.1.4.1.23280.8.1.5.1~6（outputPower1~6），单位 1W → 原始值 = W
# 电能：1.3.6.1.4.1.23280.8.1.7.1~6（outputEnergy1~6），单位 1Wh → 原始值 = Wh
OID_OUTLET_CURRENT_BASE = "1.3.6.1.4.1.23280.8.1.4"
OID_OUTLET_POWER_BASE = "1.3.6.1.4.1.23280.8.1.5"
OID_OUTLET_ENERGY_BASE = "1.3.6.1.4.1.23280.8.1.7"
CURRENT_DIVISOR = 100   # 0.01A
ENERGY_KWH_DIVISOR = 1000  # Wh → kWh

try:
    from pysnmp.hlapi.v3arch.asyncio import (
        SnmpEngine,
        get_cmd,
        CommunityData,
        UdpTransportTarget,
        ContextData,
    )
    from pysnmp.smi.rfc1902 import ObjectType, ObjectIdentity
except ImportError as e:
    print(f"请先安装 pysnmp，并确保与 311.py 相同环境。错误: {e}")
    sys.exit(1)


async def get_snmp_raw(ip: str, community: str, oid: str) -> Optional[float]:
    """SNMP GET 返回原始数值。"""
    snmp_engine = SnmpEngine()
    try:
        result = await get_cmd(
            snmp_engine,
            CommunityData(community, mpModel=0),
            await UdpTransportTarget.create((ip, SNMP_PORT), timeout=3, retries=5),
            ContextData(),
            ObjectType(ObjectIdentity(oid)),
        )
        error_indication, error_status, error_index, var_binds = result
        if error_indication or error_status or not var_binds:
            return None
        return float(var_binds[0][1])
    except Exception:
        return None
    finally:
        snmp_engine.close_dispatcher()


async def get_all_outlets(ip: str, community: str) -> tuple:
    """获取 6 个插座电流(A)、功率(W)、电能(Wh/kWh)。返回 (currents, powers_w, energy_wh, energy_kwh)。"""
    currents: List[Optional[float]] = []
    powers_w: List[Optional[float]] = []
    energy_wh: List[Optional[float]] = []
    energy_kwh: List[Optional[float]] = []
    for i in range(1, OUTLETS + 1):
        oid_c = f"{OID_OUTLET_CURRENT_BASE}.{i}"
        oid_p = f"{OID_OUTLET_POWER_BASE}.{i}"
        oid_e = f"{OID_OUTLET_ENERGY_BASE}.{i}"

        raw_c = await get_snmp_raw(ip, community, oid_c)
        raw_p = await get_snmp_raw(ip, community, oid_p)
        raw_e = await get_snmp_raw(ip, community, oid_e)

        currents.append(round(raw_c / CURRENT_DIVISOR, 3) if raw_c is not None else None)
        powers_w.append(round(raw_p, 1) if raw_p is not None else None)
        energy_wh.append(round(raw_e, 1) if raw_e is not None else None)
        energy_kwh.append(round(raw_e / ENERGY_KWH_DIVISOR, 3) if raw_e is not None else None)

    return currents, powers_w, energy_wh, energy_kwh


def main():
    print(f"Pdu3 电流/功率/电能 测试 | 目标: {PDU3_IP} | 每 {INTERVAL_SEC} 秒刷新 | Ctrl+C 退出")
    print("(电流 A，功率 W，电能 Wh / kWh)\n")
    print("-" * 50)

    while True:
        ts = datetime.now().strftime("%H:%M:%S")
        currents_a, powers_w, energy_wh, energy_kwh = asyncio.run(
            get_all_outlets(PDU3_IP, SNMP_COMMUNITY)
        )

        print(f"[{ts}]")
        print("  插座    电流(A)   功率(W)   电能(Wh)   电能(kWh)")
        for i in range(OUTLETS):
            ca = f"{currents_a[i]:.3f}" if currents_a[i] is not None else "-"
            pw = f"{powers_w[i]:.1f}" if powers_w[i] is not None else "-"
            ew = f"{energy_wh[i]:.1f}" if energy_wh[i] is not None else "-"
            ek = f"{energy_kwh[i]:.3f}" if energy_kwh[i] is not None else "-"
            print(f"   {i+1}   {ca:>8}  {pw:>8}  {ew:>9}  {ek:>9}")
        total_a = sum(c for c in currents_a if c is not None)
        total_w = sum(p for p in powers_w if p is not None)
        total_wh = sum(e for e in energy_wh if e is not None)
        total_kwh = sum(e for e in energy_kwh if e is not None)
        print(
            f"  合计  {total_a:.3f}A  {total_w:.1f}W  {total_wh:.1f}Wh  {total_kwh:.3f}kWh"
        )
        print("-" * 50)

        try:
            time.sleep(INTERVAL_SEC)
        except KeyboardInterrupt:
            print("\n已退出")
            break


if __name__ == "__main__":
    main()
