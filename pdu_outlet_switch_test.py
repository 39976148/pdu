# -*- coding: utf-8 -*-
"""
Pdu3 插座开/关 测试程序 —— 验证 SNMP 控制是否生效。

功能：
- 通过 SNMP 读取/控制 Pdu3（192.168.1.163）某一个插座的开关状态。
- 使用 OID：
  - 插座状态：1.3.6.1.4.1.23280.8.1.2.X（X 为插座号 1~6），1=开，0=关。
  - 插座控制：同 8.1.2.X，通过 SET 1/0 控制开/关（如需 private community，请修改 SNMP_WRITE_COMMUNITY）。

用法（示例）：
  python pdu_outlet_switch_test.py  --outlet 2  --action on
  python pdu_outlet_switch_test.py  --outlet 4  --action off
"""

import argparse
import asyncio
import sys
from typing import Optional

PDU3_IP = "192.168.1.163"
SNMP_PORT = 161

# 根据手册：
# outletStatusState  1.3.6.1.4.1.23280.8.1.2.1~N  INTEGER  只读 继电器状态
#   1：关闭状态（断开） 2：打开状态（闭合） 不支持则返回-1
# outletControlCommand 1.3.6.1.4.1.23280.9.1.2.1~N INTEGER  读写 继电器动作
#   1：闭合继电器 2：断开继电器 3：重启继电器 4：挂锁 5：解锁  不支持则返回-1
#
# 示例：
# 控制第一个插口闭合：
#   snmpset -v1 -c private 192.168.0.91 .1.3.6.1.4.1.23280.9.1.2.1 i 1

# 读状态用 public，写控制用 private（与文档示例一致，如设备配置不同可自行调整）
SNMP_READ_COMMUNITY = "public"
SNMP_WRITE_COMMUNITY = "private"

OID_OUTLET_STATE_BASE = "1.3.6.1.4.1.23280.8.1.2"
OID_OUTLET_CONTROL_BASE = "1.3.6.1.4.1.23280.9.1.2"

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
except ImportError as e:
    print("请先安装 pysnmp: pip install pysnmp")
    print("导入错误:", e)
    sys.exit(1)


async def snmp_get_int(ip: str, community: str, oid: str) -> Optional[int]:
    """SNMP GET，返回整数或 None。"""
    engine = SnmpEngine()
    try:
        result = await get_cmd(
            engine,
            CommunityData(community, mpModel=0),
            await UdpTransportTarget.create((ip, SNMP_PORT), timeout=3, retries=2),
            ContextData(),
            ObjectType(ObjectIdentity(oid)),
        )
        err_ind, err_status, _, var_binds = result
        if err_ind or err_status or not var_binds:
            return None
        return int(var_binds[0][1])
    except Exception:
        return None
    finally:
        engine.close_dispatcher()


async def snmp_set_int(ip: str, community: str, oid: str, value: int) -> bool:
    """SNMP SET，写入整数 0/1。"""
    engine = SnmpEngine()
    try:
        result = await set_cmd(
            engine,
            CommunityData(community, mpModel=0),
            await UdpTransportTarget.create((ip, SNMP_PORT), timeout=3, retries=2),
            ContextData(),
            ObjectType(ObjectIdentity(oid), Integer32(value)),
        )
        err_ind, err_status, _, _ = result
        return not (err_ind or err_status)
    except Exception:
        return False
    finally:
        engine.close_dispatcher()


def pretty_state(v: Optional[int]) -> str:
    if v is None:
        return "未知(None)"
    if v == 1:
        return "开(1)"
    if v == 0:
        return "关(0)"
    return f"其他({v})"


async def main_async(outlet: int, action: str):
    state_oid = f"{OID_OUTLET_STATE_BASE}.{outlet}"
    ctrl_oid = f"{OID_OUTLET_CONTROL_BASE}.{outlet}"
    print(f"目标 PDU: {PDU3_IP}, 插座: {outlet}")
    print(f"状态 OID:   {state_oid}")
    print(f"控制 OID:   {ctrl_oid}")

    # 1. 读取当前状态
    before = await snmp_get_int(PDU3_IP, SNMP_READ_COMMUNITY, state_oid)
    print("当前状态:", pretty_state(before))

    # 2. 计算要写入的继电器动作：
    #  1：闭合继电器（打开电源）  2：断开继电器（关闭电源）
    if action == "toggle":
        # 当前 1=关闭 2=打开；toggle：1->2, 2->1, 其他值默认切到 2(打开)
        if before == 1:
            cmd = 2  # 从关闭切到断开?（若你希望相反，可交换 1/2）
        elif before == 2:
            cmd = 1
        else:
            cmd = 1
    elif action == "on":
        cmd = 1
    else:  # off
        cmd = 2

    print(f"写入控制命令: {cmd} (1=闭合, 2=断开)")
    ok = await snmp_set_int(PDU3_IP, SNMP_WRITE_COMMUNITY, ctrl_oid, cmd)
    print("写入结果:", "成功" if ok else "失败")

    # 3. 再次读取确认
    after = await snmp_get_int(PDU3_IP, SNMP_READ_COMMUNITY, state_oid)
    print("写入后状态:", pretty_state(after))


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Pdu3 插座开关 SNMP 控制测试")
    p.add_argument(
        "--outlet",
        type=int,
        default=1,
        help="插座编号 1~6（默认 1）",
    )
    p.add_argument(
        "--action",
        choices=["on", "off", "toggle"],
        default="toggle",
        help="操作：on=开，off=关，toggle=当前状态取反（默认）",
    )
    return p.parse_args()


def main():
    args = parse_args()
    if not (1 <= args.outlet <= 6):
        print("插座编号必须是 1~6")
        sys.exit(1)
    try:
        asyncio.run(main_async(args.outlet, args.action))
    except KeyboardInterrupt:
        print("\n已中断")


if __name__ == "__main__":
    main()

