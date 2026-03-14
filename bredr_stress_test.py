"""
bredr_stress_test.py — BR/EDR (Bluetooth Classic) stress test via BlueZ.

REQUISITOS (Linux únicamente):
  sudo apt install bluetooth bluez python3-dbus
  pip install dbus-python PyGObject

ADVERTENCIA: Este script usa conexiones BR/EDR de bajo nivel.
Úsalo exclusivamente en dispositivos propios en entornos controlados.

Técnicas implementadas:
  1. L2CAP ping flood  — pings repetidos de tamaño máximo (L2CAP echo requests)
  2. L2CAP connection storm — múltiples canales L2CAP abiertos simultáneamente
  3. RFCOMM channel scan — intenta conectar a todos los canales RFCOMM (1-30)
  4. SDP query flood  — consultas SDP repetidas (service discovery)
  5. ACL connection churn — connect/disconnect rápido a nivel ACL

Uso:
    sudo python bredr_stress_test.py <BD_ADDR>
    sudo python bredr_stress_test.py <BD_ADDR> --mode all
    sudo python bredr_stress_test.py <BD_ADDR> --mode l2ping --count 9999 --size 600
    sudo python bredr_stress_test.py <BD_ADDR> --mode rfcomm --workers 5
"""

import argparse
import asyncio
import os
import socket
import struct
import subprocess
import sys
import time
from dataclasses import dataclass, field


# ---------------------------------------------------------------------------
# Guards
# ---------------------------------------------------------------------------

if sys.platform != "linux":
    print("ERROR: bredr_stress_test.py solo funciona en Linux (requiere BlueZ/HCI).")
    sys.exit(1)

if os.geteuid() != 0:
    print("ERROR: Se requieren permisos root (ejecuta con sudo).")
    sys.exit(1)


# ---------------------------------------------------------------------------
# Data model
# ---------------------------------------------------------------------------

@dataclass
class PingResult:
    seq: int
    success: bool
    rtt_ms: float | None
    error: str | None


@dataclass
class L2capConnResult:
    psm: int
    success: bool
    error: str | None


@dataclass
class RfcommScanResult:
    channel: int
    reachable: bool
    error: str | None


@dataclass
class BredrStressReport:
    address: str
    ping_sent: int
    ping_ok: int
    ping_rtt_avg_ms: float | None
    l2cap_channels_ok: int
    l2cap_channels_fail: int
    rfcomm_reachable: list[int]
    sdp_queries_ok: int
    sdp_queries_fail: int
    acl_churns: int
    acl_churn_ok: int
    pings: list[PingResult] = field(default_factory=list)


# ---------------------------------------------------------------------------
# 1. L2CAP Ping flood  (AF_BLUETOOTH / BTPROTO_L2CAP echo)
# ---------------------------------------------------------------------------

L2CAP_PSM_SDP = 1

def _l2cap_ping_once(address: str, size: int, timeout: float) -> tuple[bool, float | None, str | None]:
    """Single L2CAP echo request (equivalent to l2ping -c 1)."""
    try:
        # BTPROTO_L2CAP = 0
        sock = socket.socket(socket.AF_BLUETOOTH, socket.SOCK_RAW, socket.BTPROTO_L2CAP)
        sock.settimeout(timeout)
        sock.connect((address, L2CAP_PSM_SDP))
        payload = os.urandom(min(size, 672))  # L2CAP max payload on 1Mbps = 672B
        # L2CAP echo request: type=0x08
        header = struct.pack("<BBH", 0x08, 0x00, len(payload))
        t0 = time.perf_counter()
        sock.send(header + payload)
        sock.recv(4096)
        rtt = (time.perf_counter() - t0) * 1000
        sock.close()
        return True, round(rtt, 2), None
    except Exception as exc:
        return False, None, str(exc)


async def run_l2ping_flood(
    address: str,
    count: int = 200,
    size: int = 600,
    timeout: float = 2.0,
    workers: int = 3,
    delay: float = 0.0,
) -> list[PingResult]:
    """Flood the device with concurrent L2CAP echo requests."""
    print(f"\n── L2CAP Ping Flood  ({workers} workers × {count} pings, {size}B) ──")
    results: list[PingResult] = []
    lock = asyncio.Lock()
    seq = [0]

    async def _worker() -> None:
        for _ in range(count):
            async with lock:
                seq[0] += 1
                s = seq[0]
            ok, rtt, err = await asyncio.get_event_loop().run_in_executor(
                None, _l2cap_ping_once, address, size, timeout
            )
            r = PingResult(seq=s, success=ok, rtt_ms=rtt, error=err)
            async with lock:
                results.append(r)
                mark = "✓" if ok else "✗"
                rtt_s = f"{rtt:.1f}ms" if rtt else (err[:50] if err else "—")
                print(f"  [{s:>5}] {mark}  {rtt_s}")
            if delay > 0:
                await asyncio.sleep(delay)

    await asyncio.gather(*[_worker() for _ in range(workers)])
    ok_list = [r for r in results if r.success]
    lats = [r.rtt_ms for r in ok_list if r.rtt_ms]
    avg = round(sum(lats) / len(lats), 2) if lats else None
    print(f"\n  Pings OK: {len(ok_list)}/{len(results)}  RTT prom: {avg}ms")
    return results


# ---------------------------------------------------------------------------
# 2. L2CAP Connection storm — abre múltiples PSMs simultáneamente
# ---------------------------------------------------------------------------

# Common BR/EDR L2CAP PSMs
L2CAP_PSMS = [0x0001, 0x0003, 0x000F, 0x0011, 0x0019, 0x001B, 0x001D, 0x001F]


def _l2cap_connect(address: str, psm: int, hold: float, timeout: float) -> L2capConnResult:
    try:
        sock = socket.socket(socket.AF_BLUETOOTH, socket.SOCK_SEQPACKET, socket.BTPROTO_L2CAP)
        sock.settimeout(timeout)
        sock.connect((address, psm))
        time.sleep(hold)
        sock.close()
        return L2capConnResult(psm=psm, success=True, error=None)
    except Exception as exc:
        return L2capConnResult(psm=psm, success=False, error=str(exc))


async def run_l2cap_storm(
    address: str,
    cycles: int = 100,
    hold_s: float = 0.1,
    timeout: float = 3.0,
) -> tuple[int, int]:
    """Open all known PSMs simultaneously, hold, close — repeat."""
    print(f"\n── L2CAP Connection Storm  ({cycles} ciclos, {len(L2CAP_PSMS)} PSMs) ──")
    loop = asyncio.get_event_loop()
    total_ok = total_fail = 0

    for cycle in range(1, cycles + 1):
        batch = await asyncio.gather(
            *[
                loop.run_in_executor(None, _l2cap_connect, address, psm, hold_s, timeout)
                for psm in L2CAP_PSMS
            ]
        )
        ok = sum(1 for r in batch if r.success)
        fail = len(batch) - ok
        total_ok += ok
        total_fail += fail
        open_psms = [hex(r.psm) for r in batch if r.success]
        print(f"  [{cycle:>4}/{cycles}]  OK: {ok}/{len(L2CAP_PSMS)}  PSMs: {open_psms}")

    print(f"\n  Total abiertos: {total_ok}  Fallidos: {total_fail}")
    return total_ok, total_fail


# ---------------------------------------------------------------------------
# 3. RFCOMM channel scan + flood
# ---------------------------------------------------------------------------

def _rfcomm_connect(address: str, channel: int, hold: float, timeout: float) -> RfcommScanResult:
    try:
        sock = socket.socket(socket.AF_BLUETOOTH, socket.SOCK_STREAM, socket.BTPROTO_RFCOMM)
        sock.settimeout(timeout)
        sock.connect((address, channel))
        time.sleep(hold)
        sock.close()
        return RfcommScanResult(channel=channel, reachable=True, error=None)
    except Exception as exc:
        return RfcommScanResult(channel=channel, reachable=False, error=str(exc))


async def run_rfcomm_scan(
    address: str,
    channels: range = range(1, 31),
    hold_s: float = 0.2,
    timeout: float = 3.0,
    workers: int = 5,
) -> list[int]:
    """Scan all RFCOMM channels concurrently."""
    print(f"\n── RFCOMM Channel Scan  (canales {channels.start}–{channels.stop - 1}, {workers} workers) ──")
    loop = asyncio.get_event_loop()
    sem = asyncio.Semaphore(workers)

    async def _probe(ch: int) -> RfcommScanResult:
        async with sem:
            return await loop.run_in_executor(
                None, _rfcomm_connect, address, ch, hold_s, timeout
            )

    results = await asyncio.gather(*[_probe(ch) for ch in channels])
    open_channels = [r.channel for r in results if r.reachable]
    for r in results:
        mark = "✓ ABIERTO" if r.reachable else "✗"
        print(f"  Canal {r.channel:>3}: {mark}")
    print(f"\n  Canales RFCOMM abiertos: {open_channels}")
    return open_channels


# ---------------------------------------------------------------------------
# 4. SDP Query flood (via sdptool subprocess)
# ---------------------------------------------------------------------------

async def run_sdp_flood(
    address: str,
    count: int = 200,
    workers: int = 5,
) -> tuple[int, int]:
    """Flood SDP service discovery queries using sdptool."""
    print(f"\n── SDP Query Flood  ({workers} workers × {count} consultas) ──")

    # Verify sdptool is available
    result = subprocess.run(["which", "sdptool"], capture_output=True)
    if result.returncode != 0:
        print("  ⚠  sdptool no encontrado (instala bluez-tools). Saltando.")
        return 0, 0

    ok_count = [0]
    fail_count = [0]
    lock = asyncio.Lock()

    async def _query() -> None:
        for _ in range(count):
            proc = await asyncio.create_subprocess_exec(
                "sdptool", "browse", "--nobreaker", address,
                stdout=asyncio.subprocess.DEVNULL,
                stderr=asyncio.subprocess.DEVNULL,
            )
            ret = await proc.wait()
            async with lock:
                if ret == 0:
                    ok_count[0] += 1
                    print(f"  SDP OK  #{ok_count[0] + fail_count[0]}")
                else:
                    fail_count[0] += 1
                    print(f"  SDP ✗   #{ok_count[0] + fail_count[0]}")

    await asyncio.gather(*[_query() for _ in range(workers)])
    print(f"\n  SDP OK: {ok_count[0]}  Fallidas: {fail_count[0]}")
    return ok_count[0], fail_count[0]


# ---------------------------------------------------------------------------
# 5. ACL Connection churn (hcitool cc)
# ---------------------------------------------------------------------------

async def run_acl_churn(
    address: str,
    count: int = 100,
    hold_s: float = 0.1,
) -> tuple[int, int]:
    """
    Rapid ACL connect/disconnect using hcitool cc / hcitool dc.
    This stresses the ACL connection manager in the remote controller.
    """
    print(f"\n── ACL Connection Churn  ({count} ciclos, hold {hold_s}s) ──")

    for cmd in ["hcitool"]:
        if subprocess.run(["which", cmd], capture_output=True).returncode != 0:
            print(f"  ⚠  {cmd} no encontrado. Saltando.")
            return 0, 0

    ok = fail = 0
    for i in range(1, count + 1):
        # Connect
        r = await asyncio.create_subprocess_exec(
            "hcitool", "cc", "--role=m", address,
            stdout=asyncio.subprocess.DEVNULL,
            stderr=asyncio.subprocess.DEVNULL,
        )
        ret = await r.wait()
        if ret == 0:
            await asyncio.sleep(hold_s)
            # Disconnect
            d = await asyncio.create_subprocess_exec(
                "hcitool", "dc", address,
                stdout=asyncio.subprocess.DEVNULL,
                stderr=asyncio.subprocess.DEVNULL,
            )
            await d.wait()
            ok += 1
            print(f"  [{i:>4}/{count}] ✓  connect+disconnect")
        else:
            fail += 1
            print(f"  [{i:>4}/{count}] ✗  connect falló")

    print(f"\n  ACL churn OK: {ok}  Fallidos: {fail}")
    return ok, fail


# ---------------------------------------------------------------------------
# Orchestrator
# ---------------------------------------------------------------------------

async def run_all(args: argparse.Namespace) -> None:
    addr = args.address
    sep = "═" * 72
    print(sep)
    print(f" BR/EDR Stress Test — {addr}")
    print(sep)

    report = BredrStressReport(
        address=addr,
        ping_sent=0, ping_ok=0, ping_rtt_avg_ms=None,
        l2cap_channels_ok=0, l2cap_channels_fail=0,
        rfcomm_reachable=[],
        sdp_queries_ok=0, sdp_queries_fail=0,
        acl_churns=args.count, acl_churn_ok=0,
    )

    mode = args.mode

    if mode in ("l2ping", "all"):
        pings = await run_l2ping_flood(
            addr,
            count=args.count,
            size=args.size,
            timeout=args.timeout,
            workers=args.workers,
            delay=args.delay,
        )
        report.pings = pings
        report.ping_sent = len(pings)
        report.ping_ok = sum(1 for p in pings if p.success)
        lats = [p.rtt_ms for p in pings if p.success and p.rtt_ms]
        report.ping_rtt_avg_ms = round(sum(lats) / len(lats), 2) if lats else None

    if mode in ("l2storm", "all"):
        ok, fail = await run_l2cap_storm(
            addr, cycles=args.count, hold_s=0.1, timeout=args.timeout
        )
        report.l2cap_channels_ok = ok
        report.l2cap_channels_fail = fail

    if mode in ("rfcomm", "all"):
        report.rfcomm_reachable = await run_rfcomm_scan(
            addr, workers=args.workers, timeout=args.timeout
        )

    if mode in ("sdp", "all"):
        report.sdp_queries_ok, report.sdp_queries_fail = await run_sdp_flood(
            addr, count=args.count, workers=args.workers
        )

    if mode in ("acl", "all"):
        report.acl_churn_ok, _ = await run_acl_churn(
            addr, count=args.count, hold_s=args.delay if args.delay > 0 else 0.1
        )

    # Final summary
    print()
    print(sep)
    print(f" RESUMEN FINAL — {addr}")
    print(sep)
    if report.ping_sent:
        print(
            f"  L2CAP pings      : {report.ping_ok}/{report.ping_sent}"
            f"  RTT prom: {report.ping_rtt_avg_ms}ms"
        )
    if report.l2cap_channels_ok + report.l2cap_channels_fail:
        print(
            f"  L2CAP storm      : {report.l2cap_channels_ok} ok  "
            f"{report.l2cap_channels_fail} fail"
        )
    if report.rfcomm_reachable is not None:
        print(f"  RFCOMM abiertos  : {report.rfcomm_reachable}")
    if report.sdp_queries_ok + report.sdp_queries_fail:
        print(
            f"  SDP queries      : {report.sdp_queries_ok} ok  "
            f"{report.sdp_queries_fail} fail"
        )
    if report.acl_churn_ok:
        print(f"  ACL churns OK    : {report.acl_churn_ok}")
    print(sep)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="BR/EDR stress test via BlueZ (Linux, requiere root)",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument("address", metavar="BD_ADDR",
                        help="Dirección BD (formato AA:BB:CC:DD:EE:FF)")
    parser.add_argument(
        "--mode",
        choices=["l2ping", "l2storm", "rfcomm", "sdp", "acl", "all"],
        default="all",
        help=(
            "l2ping  = L2CAP echo flood  |  "
            "l2storm = abrir múltiples PSMs  |  "
            "rfcomm  = escanear canales RFCOMM  |  "
            "sdp     = flood SDP queries  |  "
            "acl     = churn ACL connect/disconnect  |  "
            "all     = todos"
        ),
    )
    parser.add_argument("--count",   type=int,   default=200,  metavar="N",
                        help="Número de operaciones por modo")
    parser.add_argument("--size",    type=int,   default=600,  metavar="BYTES",
                        help="Tamaño del payload L2CAP ping (máx 672)")
    parser.add_argument("--workers", type=int,   default=3,    metavar="W",
                        help="Workers concurrentes")
    parser.add_argument("--timeout", type=float, default=3.0,  metavar="SEG",
                        help="Timeout por operación")
    parser.add_argument("--delay",   type=float, default=0.0,  metavar="SEG",
                        help="Pausa entre operaciones (0 = sin pausa)")
    return parser.parse_args()


if __name__ == "__main__":
    asyncio.run(run_all(_parse_args()))
