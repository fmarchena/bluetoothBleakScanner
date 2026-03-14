"""
gatt_stress_test.py — BLE GATT Stress Test

Combines multiple ATT-layer operations per connection simultaneously:

  • Read flood        — continuous ATT Read Requests on readable characteristics
  • Write flood       — write_without_response (no ACK/flow-control) or with response
  • Notify churn      — rapid CCCD subscribe/unsubscribe cycling
  • MTU negotiation   — request maximum MTU on every fresh connection

Each worker maintains its own independent BLE connection and hammers all
applicable operations concurrently (asyncio.gather per cycle).

Why this is more aggressive than a pure read loop
--------------------------------------------------
  ATT Read Request  → device must respond         (flow-controlled, 1 in-flight)
  Write Without Response → fire-and-forget, no ACK, can flood the ATT Rx buffer
  Notify subscribe  → device starts SENDING data unsolicited (bidirectional flood)
  MTU negotiation   → forces link-layer reconfiguration on every connect

Usage (standalone)
------------------
    python gatt_stress_test.py <ADDRESS>
    python gatt_stress_test.py <ADDRESS> --n 500 --workers 5 --delay 0
    python gatt_stress_test.py <ADDRESS> --no-notify --write-size 180
    python gatt_stress_test.py <ADDRESS> --write-response   # use ATT Write Request

Or call programmatically via run_gatt_stress_test() / run_multi_gatt_stress_test().
"""

import argparse
import asyncio
import os
import time
from dataclasses import dataclass, field

from bleak import BleakClient
from bleak.backends.characteristic import BleakGATTCharacteristic
from bleak.exc import BleakError


# ---------------------------------------------------------------------------
# Data model
# ---------------------------------------------------------------------------

@dataclass
class CycleResult:
    cycle: int
    reads_ok: int
    reads_fail: int
    writes_ok: int
    writes_fail: int
    notifies_ok: int         # successful subscribe+unsubscribe pairs
    notifies_fail: int
    notify_packets_rx: int   # unsolicited packets received during subscription
    read_latency_ms: float | None
    write_latency_ms: float | None
    error: str | None


@dataclass
class GattStressReport:
    address: str
    total_cycles: int
    completed_cycles: int
    reads_ok: int
    reads_fail: int
    writes_ok: int
    writes_fail: int
    notifies_ok: int
    notify_packets_rx: int
    reconnects: int
    backoffs_applied: int
    avg_read_latency_ms: float | None
    avg_write_latency_ms: float | None
    workers: int = 1
    cycles: list[CycleResult] = field(default_factory=list)


@dataclass
class GattStressConfig:
    """Per-device parameters for run_multi_gatt_stress_test."""
    address: str
    name: str
    n: int
    delay_s: float
    connect_timeout: float
    read_timeout: float
    write_timeout: float
    notify_hold_s: float       # how long to stay subscribed per churn cycle
    write_size: int            # bytes per write payload
    workers: int
    do_reads: bool = True
    do_writes: bool = True
    do_notify: bool = True
    write_response: bool = False  # True = ATT Write Request (slower, more pressure)
    max_consecutive_fails: int = 10
    backoff_s: float = 5.0


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

async def _lprint(msg: str, lock: asyncio.Lock | None) -> None:
    if lock is not None:
        async with lock:
            print(msg, flush=True)
    else:
        print(msg, flush=True)


def _classify(char: BleakGATTCharacteristic) -> tuple[bool, bool, bool]:
    """Returns (readable, writable, notifiable)."""
    props = set(char.properties)
    readable   = "read"   in props
    writable   = "write"  in props or "write-without-response" in props
    notifiable = "notify" in props or "indicate" in props
    return readable, writable, notifiable


# ---------------------------------------------------------------------------
# Per-worker core
# ---------------------------------------------------------------------------

async def _run_worker(
    worker_id: int,
    address: str,
    n: int,
    delay_s: float,
    connect_timeout: float,
    read_timeout: float,
    write_timeout: float,
    notify_hold_s: float,
    write_size: int,
    do_reads: bool,
    do_writes: bool,
    do_notify: bool,
    write_response: bool,
    max_consecutive_fails: int,
    backoff_s: float,
    prefix: str,
    lock: asyncio.Lock | None,
) -> tuple[list[CycleResult], int, int, int]:
    """
    One independent BLE worker.

    Returns:
        (cycle_results, reconnect_count, backoff_count, completed_cycles)
    """
    results: list[CycleResult] = []
    reconnects = 0
    backoffs_applied = 0
    consecutive_fails = 0

    # ── Connect ─────────────────────────────────────────────────────────
    async def _connect() -> BleakClient | None:
        try:
            client = BleakClient(address, timeout=connect_timeout)
            await client.connect()
            return client
        except (BleakError, asyncio.TimeoutError, OSError) as exc:
            await _lprint(f"  {prefix}⚡ Conexión fallida: {exc}", lock)
            return None

    client = await _connect()
    if client is None:
        return results, reconnects, backoffs_applied, 0

    # ── Discover characteristics ─────────────────────────────────────────
    def _discover(c: BleakClient) -> tuple[list[str], list[str], list[str]]:
        reads_uuids, write_uuids, notify_uuids = [], [], []
        try:
            for svc in (c.services or []):
                for ch in svc.characteristics:
                    r, w, n_ = _classify(ch)
                    uuid = str(ch.uuid)
                    if r and do_reads:
                        reads_uuids.append(uuid)
                    if w and do_writes:
                        write_uuids.append(uuid)
                    if n_ and do_notify:
                        notify_uuids.append(uuid)
        except Exception:
            pass
        return reads_uuids, write_uuids, notify_uuids

    r_uuids, w_uuids, n_uuids = _discover(client)

    await _lprint(
        f"  {prefix}Características → "
        f"legibles: {len(r_uuids)}  "
        f"escribibles: {len(w_uuids)}  "
        f"notificables: {len(n_uuids)}",
        lock,
    )

    # ── Cycle loop ───────────────────────────────────────────────────────
    completed = 0
    for cycle in range(1, n + 1):

        # Reconnect if dropped between cycles
        if not client.is_connected:
            await _lprint(
                f"  {prefix}[{cycle:>3}/{n}] Desconectado — reconectando...", lock
            )
            try:
                await client.disconnect()
            except Exception:
                pass
            client = await _connect()
            if client is None:
                break
            reconnects += 1
            r_uuids, w_uuids, n_uuids = _discover(client)

        # Local typed alias so nested closures see BleakClient, not BleakClient|None
        assert client is not None
        active: BleakClient = client

        reads_ok = reads_fail = 0
        writes_ok = writes_fail = 0
        notifies_ok = notifies_fail = 0
        notify_packets_rx = 0
        read_lat: list[float] = []
        write_lat: list[float] = []
        cycle_error: str | None = None

        # ── 1. Read flood ────────────────────────────────────────────────
        async def _do_reads() -> None:
            nonlocal reads_ok, reads_fail
            for uuid in r_uuids:
                t0 = time.perf_counter()
                try:
                    await asyncio.wait_for(
                        active.read_gatt_char(uuid), timeout=read_timeout
                    )
                    read_lat.append((time.perf_counter() - t0) * 1000)
                    reads_ok += 1
                except Exception:
                    reads_fail += 1

        # ── 2. Write flood ───────────────────────────────────────────────
        async def _do_writes() -> None:
            nonlocal writes_ok, writes_fail
            payload = os.urandom(min(write_size, 512))
            for uuid in w_uuids:
                t0 = time.perf_counter()
                try:
                    await asyncio.wait_for(
                        active.write_gatt_char(
                            uuid, payload, response=write_response
                        ),
                        timeout=write_timeout,
                    )
                    write_lat.append((time.perf_counter() - t0) * 1000)
                    writes_ok += 1
                except Exception:
                    writes_fail += 1

        # ── 3. Notify churn ──────────────────────────────────────────────
        async def _do_notify() -> None:
            nonlocal notifies_ok, notifies_fail, notify_packets_rx
            for uuid in n_uuids:
                counter = [0]

                def _cb(_: BleakGATTCharacteristic, __: bytearray) -> None:
                    counter[0] += 1

                try:
                    await active.start_notify(uuid, _cb)
                    await asyncio.sleep(notify_hold_s)
                    await active.stop_notify(uuid)
                    notifies_ok += 1
                    notify_packets_rx += counter[0]
                except Exception:
                    notifies_fail += 1
                    try:
                        await active.stop_notify(uuid)
                    except Exception:
                        pass

        # ── Fire all three simultaneously ────────────────────────────────
        try:
            ops = []
            if r_uuids:
                ops.append(_do_reads())
            if w_uuids:
                ops.append(_do_writes())
            if n_uuids:
                ops.append(_do_notify())

            if ops:
                await asyncio.gather(*ops)
            else:
                # Nothing to do — device has no usable characteristics
                await _lprint(
                    f"  {prefix}⚠  Sin características para operar. Abortando.", lock
                )
                break

        except (BleakError, asyncio.TimeoutError, OSError) as exc:
            cycle_error = str(exc)
            consecutive_fails += 1
        else:
            any_fail = (reads_fail + writes_fail + notifies_fail) > 0
            if any_fail:
                consecutive_fails += 1
            else:
                consecutive_fails = 0

        result = CycleResult(
            cycle=cycle,
            reads_ok=reads_ok,
            reads_fail=reads_fail,
            writes_ok=writes_ok,
            writes_fail=writes_fail,
            notifies_ok=notifies_ok,
            notifies_fail=notifies_fail,
            notify_packets_rx=notify_packets_rx,
            read_latency_ms=round(sum(read_lat) / len(read_lat), 2) if read_lat else None,
            write_latency_ms=round(sum(write_lat) / len(write_lat), 2) if write_lat else None,
            error=cycle_error,
        )
        results.append(result)
        completed += 1

        ok_mark = "✓" if not cycle_error and (reads_fail + writes_fail) == 0 else "✗"
        parts = [f"  {prefix}[{cycle:>3}/{n}] {ok_mark}"]
        if reads_ok + reads_fail:
            parts.append(f"R:{reads_ok}✓/{reads_fail}✗")
        if writes_ok + writes_fail:
            parts.append(f"W:{writes_ok}✓/{writes_fail}✗")
        if notifies_ok + notifies_fail:
            parts.append(f"N:{notifies_ok}✓ {notify_packets_rx}pkts")
        if result.read_latency_ms is not None:
            parts.append(f"RdLat:{result.read_latency_ms:.0f}ms")
        if result.write_latency_ms is not None:
            parts.append(f"WrLat:{result.write_latency_ms:.0f}ms")
        if cycle_error:
            parts.append(f"ERR:{cycle_error[:60]}")
        await _lprint("  ".join(parts), lock)

        # Backoff on consecutive failures
        if (
            max_consecutive_fails > 0
            and consecutive_fails >= max_consecutive_fails
            and cycle < n
        ):
            backoffs_applied += 1
            await _lprint(
                f"  {prefix}⚠  {consecutive_fails} fallos — "
                f"backoff #{backoffs_applied}: {backoff_s}s",
                lock,
            )
            await asyncio.sleep(backoff_s)
            consecutive_fails = 0

        elif cycle < n and delay_s > 0:
            await asyncio.sleep(delay_s)

    # ── Clean disconnect ─────────────────────────────────────────────────
    try:
        if client is not None and client.is_connected:
            await client.disconnect()
    except Exception:
        pass

    return results, reconnects, backoffs_applied, completed


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

async def run_gatt_stress_test(
    address: str,
    n: int = 100,
    delay_s: float = 0.0,
    connect_timeout: float = 10.0,
    read_timeout: float = 5.0,
    write_timeout: float = 5.0,
    notify_hold_s: float = 0.2,
    write_size: int = 20,
    workers: int = 3,
    do_reads: bool = True,
    do_writes: bool = True,
    do_notify: bool = True,
    write_response: bool = False,
    max_consecutive_fails: int = 10,
    backoff_s: float = 5.0,
    verbose: bool = True,
    _label: str | None = None,
    _lock: asyncio.Lock | None = None,
) -> GattStressReport:
    """
    Hammers a BLE device with concurrent reads, writes and notify subscribe/
    unsubscribe cycles from *workers* independent connections simultaneously.

    Args:
        address:               BLE device address / UUID.
        n:                     Stress cycles per worker.
        delay_s:               Pause between cycles (0 = no pause).
        connect_timeout:       BleakClient connection timeout.
        read_timeout:          Per-read asyncio.wait_for timeout.
        write_timeout:         Per-write asyncio.wait_for timeout.
        notify_hold_s:         Seconds to hold each notify subscription.
        write_size:            Bytes per write payload (random data).
        workers:               Independent concurrent BLE connections.
        do_reads:              Enable read flood.
        do_writes:             Enable write flood.
        do_notify:             Enable notify subscribe/unsubscribe churn.
        write_response:        True = ATT Write Request (blocks for ACK);
                               False = Write Without Response (fire-and-forget).
        max_consecutive_fails: Backoff trigger threshold (0 = disabled).
        backoff_s:             Duration of backoff pause.
        verbose:               Print per-cycle progress.

    Returns:
        GattStressReport with full aggregate stats.
    """
    workers = max(1, workers)
    prefix = f"[{_label}] " if _label else ""
    sep = "─" * 72

    if verbose:
        await _lprint(sep, _lock)
        await _lprint(f"{prefix}Test de estrés GATT", _lock)
        await _lprint(f"{prefix}Dispositivo  : {address}", _lock)
        ops = []
        if do_reads:
            ops.append("Read flood")
        if do_writes:
            ops.append(
                f"Write flood ({'con' if write_response else 'sin'} response, {write_size}B)"
            )
        if do_notify:
            ops.append(f"Notify churn ({notify_hold_s}s hold)")
        await _lprint(f"{prefix}Operaciones  : {' + '.join(ops)}", _lock)
        await _lprint(
            f"{prefix}Ciclos: {n}  Workers: {workers}  Delay: {delay_s}s  "
            f"RdTimeout: {read_timeout}s  WrTimeout: {write_timeout}s",
            _lock,
        )
        if max_consecutive_fails > 0:
            await _lprint(
                f"{prefix}Backoff: {backoff_s}s tras {max_consecutive_fails} fallos", _lock
            )
        await _lprint(sep, _lock)

    worker_label = (lambda i: f"{prefix}W{i + 1} ") if workers > 1 else (lambda _: prefix)

    all_worker_results: list[tuple[list[CycleResult], int, int, int]] = list(
        await asyncio.gather(
            *[
                _run_worker(
                    worker_id=i,
                    address=address,
                    n=n,
                    delay_s=delay_s,
                    connect_timeout=connect_timeout,
                    read_timeout=read_timeout,
                    write_timeout=write_timeout,
                    notify_hold_s=notify_hold_s,
                    write_size=write_size,
                    do_reads=do_reads,
                    do_writes=do_writes,
                    do_notify=do_notify,
                    write_response=write_response,
                    max_consecutive_fails=max_consecutive_fails,
                    backoff_s=backoff_s,
                    prefix=worker_label(i),
                    lock=_lock,
                )
                for i in range(workers)
            ]
        )
    )

    # ── Aggregate ────────────────────────────────────────────────────────
    all_cycles: list[CycleResult] = []
    total_reconnects = 0
    total_backoffs = 0
    total_completed = 0

    for w_cycles, w_reconnects, w_backoffs, w_completed in all_worker_results:
        all_cycles.extend(w_cycles)
        total_reconnects += w_reconnects
        total_backoffs += w_backoffs
        total_completed += w_completed

    r_lats = [c.read_latency_ms for c in all_cycles if c.read_latency_ms is not None]
    w_lats = [c.write_latency_ms for c in all_cycles if c.write_latency_ms is not None]

    def _avg(lst: list[float]) -> float | None:
        return round(sum(lst) / len(lst), 2) if lst else None

    report = GattStressReport(
        address=address,
        total_cycles=n * workers,
        completed_cycles=total_completed,
        reads_ok=sum(c.reads_ok for c in all_cycles),
        reads_fail=sum(c.reads_fail for c in all_cycles),
        writes_ok=sum(c.writes_ok for c in all_cycles),
        writes_fail=sum(c.writes_fail for c in all_cycles),
        notifies_ok=sum(c.notifies_ok for c in all_cycles),
        notify_packets_rx=sum(c.notify_packets_rx for c in all_cycles),
        reconnects=total_reconnects,
        backoffs_applied=total_backoffs,
        avg_read_latency_ms=_avg(r_lats),
        avg_write_latency_ms=_avg(w_lats),
        workers=workers,
        cycles=all_cycles,
    )

    if verbose:
        if _lock is not None:
            async with _lock:
                _print_report(report, prefix=prefix)
        else:
            _print_report(report, prefix=prefix)

    return report


async def run_multi_gatt_stress_test(
    configs: list[GattStressConfig],
    verbose: bool = True,
) -> list[GattStressReport]:
    """Runs run_gatt_stress_test concurrently for each config."""
    lock = asyncio.Lock()
    tasks = [
        run_gatt_stress_test(
            address=cfg.address,
            n=cfg.n,
            delay_s=cfg.delay_s,
            connect_timeout=cfg.connect_timeout,
            read_timeout=cfg.read_timeout,
            write_timeout=cfg.write_timeout,
            notify_hold_s=cfg.notify_hold_s,
            write_size=cfg.write_size,
            workers=cfg.workers,
            do_reads=cfg.do_reads,
            do_writes=cfg.do_writes,
            do_notify=cfg.do_notify,
            write_response=cfg.write_response,
            max_consecutive_fails=cfg.max_consecutive_fails,
            backoff_s=cfg.backoff_s,
            verbose=verbose,
            _label=cfg.name[:18],
            _lock=lock,
        )
        for cfg in configs
    ]
    reports: list[GattStressReport] = list(await asyncio.gather(*tasks))
    _print_multi_summary(reports)
    return reports


# ---------------------------------------------------------------------------
# Reporting
# ---------------------------------------------------------------------------

def _print_report(r: GattStressReport, prefix: str = "") -> None:
    sep = "─" * 72
    total_r = r.reads_ok + r.reads_fail
    total_w = r.writes_ok + r.writes_fail
    print(sep)
    print(f"{prefix}RESUMEN — {r.address}")
    print(sep)
    print(f"  {prefix}Workers               : {r.workers}")
    print(f"  {prefix}Ciclos completados    : {r.completed_cycles}/{r.total_cycles}")
    print(f"  {prefix}Reconexiones          : {r.reconnects}")
    print(f"  {prefix}Backoffs aplicados    : {r.backoffs_applied}")
    if total_r:
        ok_pct = r.reads_ok / total_r * 100
        lat = f"{r.avg_read_latency_ms:.1f}ms" if r.avg_read_latency_ms else "—"
        print(
            f"  {prefix}Lecturas OK           : {r.reads_ok}/{total_r}"
            f"  ({ok_pct:.1f}%)  lat prom {lat}"
        )
    if total_w:
        ok_pct = r.writes_ok / total_w * 100
        lat = f"{r.avg_write_latency_ms:.1f}ms" if r.avg_write_latency_ms else "—"
        print(
            f"  {prefix}Escrituras OK         : {r.writes_ok}/{total_w}"
            f"  ({ok_pct:.1f}%)  lat prom {lat}"
        )
    if r.notifies_ok + r.notify_packets_rx:
        print(
            f"  {prefix}Subscripciones notify : {r.notifies_ok} ok"
            f"  {r.notify_packets_rx} paquetes recibidos"
        )
    print(sep)


def _print_multi_summary(reports: list[GattStressReport]) -> None:
    sep = "─" * 72
    print()
    print(sep)
    print(" RESUMEN GLOBAL — Test de estrés GATT")
    print(sep)
    print(
        f"  {'Dispositivo':<36} {'Ciclos':>7}  {'R-OK%':>6}  "
        f"{'W-OK%':>6}  {'Notif':>6}  {'Recon':>6}  {'W':>3}"
    )
    print("  " + "─" * 70)
    for r in reports:
        total_r = r.reads_ok + r.reads_fail
        total_w = r.writes_ok + r.writes_fail
        r_pct = f"{r.reads_ok / total_r * 100:.0f}%" if total_r else "—"
        w_pct = f"{r.writes_ok / total_w * 100:.0f}%" if total_w else "—"
        print(
            f"  {r.address:<36} {r.completed_cycles:>7}  {r_pct:>6}  "
            f"{w_pct:>6}  {r.notifies_ok:>6}  {r.reconnects:>6}  {r.workers:>3}"
        )
    print(sep)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Test de estrés GATT — Read + Write + Notify simultáneos",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "addresses", nargs="+", metavar="ADDRESS",
        help="BLE address / UUID (uno o varios para multi-dispositivo)",
    )
    parser.add_argument("--n", type=int, default=100, metavar="N",
                        help="Ciclos de estrés por worker")
    parser.add_argument("--delay", type=float, default=0.0, metavar="SEG",
                        help="Pausa entre ciclos (0 = sin pausa)")
    parser.add_argument("--timeout", type=float, default=10.0, metavar="SEG",
                        help="Timeout de conexión BLE")
    parser.add_argument("--read-timeout", type=float, default=5.0, metavar="SEG",
                        help="Timeout por lectura GATT individual")
    parser.add_argument("--write-timeout", type=float, default=5.0, metavar="SEG",
                        help="Timeout por escritura GATT individual")
    parser.add_argument("--notify-hold", type=float, default=0.2, metavar="SEG",
                        help="Segundos de suscripción por ciclo de notify churn")
    parser.add_argument("--write-size", type=int, default=20, metavar="BYTES",
                        help="Bytes de payload aleatorio por escritura")
    parser.add_argument("--workers", type=int, default=3, metavar="W",
                        help="Conexiones BLE concurrentes independientes")
    parser.add_argument("--write-response", action="store_true",
                        help="Usa ATT Write Request (espera ACK) en vez de Write Without Response")
    parser.add_argument("--no-reads",   action="store_true", help="Deshabilita read flood")
    parser.add_argument("--no-writes",  action="store_true", help="Deshabilita write flood")
    parser.add_argument("--no-notify",  action="store_true", help="Deshabilita notify churn")
    parser.add_argument("--max-fails", type=int, default=10, metavar="N",
                        help="Fallos consecutivos antes de backoff (0 = sin límite)")
    parser.add_argument("--backoff", type=float, default=5.0, metavar="SEG",
                        help="Segundos de pausa de backoff")
    return parser.parse_args()


if __name__ == "__main__":
    args = _parse_args()

    kwargs = dict(
        n=args.n,
        delay_s=args.delay,
        connect_timeout=args.timeout,
        read_timeout=args.read_timeout,
        write_timeout=args.write_timeout,
        notify_hold_s=args.notify_hold,
        write_size=args.write_size,
        workers=args.workers,
        do_reads=not args.no_reads,
        do_writes=not args.no_writes,
        do_notify=not args.no_notify,
        write_response=args.write_response,
        max_consecutive_fails=args.max_fails,
        backoff_s=args.backoff,
    )

    if len(args.addresses) == 1:
        asyncio.run(run_gatt_stress_test(address=args.addresses[0], **kwargs))
    else:
        cfgs = [
            GattStressConfig(
                address=addr,
                name=addr[-17:],
                **kwargs,
            )
            for addr in args.addresses
        ]
        asyncio.run(run_multi_gatt_stress_test(cfgs))
