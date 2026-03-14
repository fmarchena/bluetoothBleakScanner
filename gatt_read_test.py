"""
gatt_read_test.py — BLE GATT Read Loop test.

Connects to a BLE device, discovers all readable GATT characteristics and
performs N consecutive read cycles (one read per characteristic per cycle).
Designed to measure read latency, reliability over time, data size and
detect unexpected disconnections.

Usage (standalone):
    python gatt_read_test.py <address> [--n 50] [--delay 0.5]
                                        [--read-timeout 5.0] [--timeout 15.0]
                                        [--workers 1]
                                        [--chars <UUID1> [<UUID2> ...]]
                                        [--max-fails 10] [--backoff 5.0]

Or called programmatically via run_gatt_read_test() / run_multi_gatt_read_test().
"""

import argparse
import asyncio
import time
from dataclasses import dataclass, field

from bleak import BleakClient
from bleak.backends.characteristic import BleakGATTCharacteristic
from bleak.exc import BleakError


# ---------------------------------------------------------------------------
# Data model
# ---------------------------------------------------------------------------

@dataclass
class ReadResult:
    """Result of one GATT read on a single characteristic inside one cycle."""
    cycle: int
    char_uuid: str
    success: bool
    latency_ms: float | None
    bytes_read: int
    error: str | None


@dataclass
class GattReadReport:
    """Aggregate report produced by run_gatt_read_test."""
    address: str
    char_uuids_tested: list[str]   # discovered and actually read
    total_cycles: int              # n × workers (intended)
    completed_cycles: int          # cycles that at least started (may be < total on early drop)
    total_reads: int
    successful_reads: int
    failed_reads: int
    success_rate: float
    min_latency_ms: float | None
    max_latency_ms: float | None
    avg_latency_ms: float | None
    total_bytes_read: int
    reconnects: int                # how many times the worker had to reconnect
    backoffs_applied: int
    workers: int = 1
    reads: list[ReadResult] = field(default_factory=list)


@dataclass
class GattReadConfig:
    """Per-device parameters for run_multi_gatt_read_test."""
    address: str
    name: str
    n: int
    delay_s: float
    connect_timeout: float
    read_timeout: float
    workers: int
    char_uuids: list[str]          # empty list = all readable characteristics
    max_consecutive_fails: int = 10
    backoff_s: float = 5.0


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

async def _lprint(msg: str, lock: asyncio.Lock | None) -> None:
    if lock is not None:
        async with lock:
            print(msg, flush=True)
    else:
        print(msg, flush=True)


def _is_readable(char: BleakGATTCharacteristic) -> bool:
    return "read" in char.properties


async def _try_connect(
    address: str,
    connect_timeout: float,
    prefix: str,
    lock: asyncio.Lock | None,
) -> BleakClient | None:
    """Single connection attempt; returns BleakClient on success, None on failure."""
    try:
        client = BleakClient(address, timeout=connect_timeout)
        await client.connect()
        return client
    except (BleakError, asyncio.TimeoutError, OSError) as exc:
        await _lprint(f"  {prefix}⚡ No se pudo conectar: {exc}", lock)
        return None


async def _discover_readable(
    client: BleakClient,
    char_uuids_filter: list[str],
    prefix: str,
    lock: asyncio.Lock | None,
) -> list[str]:
    """
    Returns the UUIDs of readable characteristics, optionally filtered.
    Returns an empty list if discovery fails.
    """
    try:
        services = client.services or []
        candidates: list[BleakGATTCharacteristic] = [
            char
            for svc in services
            for char in svc.characteristics
            if _is_readable(char)
        ]
        if char_uuids_filter:
            filter_set = {u.lower() for u in char_uuids_filter}
            candidates = [c for c in candidates if str(c.uuid).lower() in filter_set]
        return [str(c.uuid) for c in candidates]
    except Exception as exc:
        await _lprint(f"  {prefix}⚡ Error al descubrir servicios: {exc}", lock)
        return []


async def _run_worker(
    worker_id: int,
    address: str,
    n: int,
    delay_s: float,
    connect_timeout: float,
    read_timeout: float,
    char_uuids_filter: list[str],
    max_consecutive_fails: int,
    backoff_s: float,
    prefix: str,
    lock: asyncio.Lock | None,
) -> tuple[list[ReadResult], list[str], int, int, int]:
    """
    One independent worker: maintains a persistent BLE connection and performs
    N read cycles across all selected characteristics.

    Returns:
        (reads, char_uuids_tested, reconnect_count, backoff_count, completed_cycles)
    """
    reads: list[ReadResult] = []
    reconnects = 0
    backoffs_applied = 0
    consecutive_fails = 0
    char_uuids: list[str] = []

    # ── Initial connection ────────────────────────────────────────────────
    client = await _try_connect(address, connect_timeout, prefix, lock)
    if client is None:
        return reads, char_uuids, reconnects, backoffs_applied, 0

    char_uuids = await _discover_readable(client, char_uuids_filter, prefix, lock)
    if not char_uuids:
        await _lprint(f"  {prefix}⚠  No se encontraron características legibles.", lock)
        try:
            await client.disconnect()
        except Exception:
            pass
        return reads, char_uuids, reconnects, backoffs_applied, 0

    await _lprint(
        f"  {prefix}Características ({len(char_uuids)}): " + ", ".join(char_uuids),
        lock,
    )

    # ── Read loop ─────────────────────────────────────────────────────────
    completed_cycles = 0
    for cycle in range(1, n + 1):

        # Reconnect if the connection was lost between cycles
        if not client.is_connected:
            await _lprint(
                f"  {prefix}[ciclo {cycle:>3}/{n}] Desconectado — reconectando...", lock
            )
            try:
                await client.disconnect()
            except Exception:
                pass
            client = await _try_connect(address, connect_timeout, prefix, lock)
            if client is None:
                break
            reconnects += 1
            # Re-discover in case the service layout changed after reconnect
            char_uuids = await _discover_readable(
                client, char_uuids_filter, prefix, lock
            )
            if not char_uuids:
                break

        cycle_ok = True
        for uuid in char_uuids:
            t0 = time.perf_counter()
            try:
                data: bytes = await asyncio.wait_for(
                    client.read_gatt_char(uuid),
                    timeout=read_timeout,
                )
                latency_ms = round((time.perf_counter() - t0) * 1000, 2)
                reads.append(
                    ReadResult(
                        cycle=cycle,
                        char_uuid=uuid,
                        success=True,
                        latency_ms=latency_ms,
                        bytes_read=len(data),
                        error=None,
                    )
                )
                consecutive_fails = 0

            except (BleakError, asyncio.TimeoutError, OSError) as exc:
                latency_ms = round((time.perf_counter() - t0) * 1000, 2)
                reads.append(
                    ReadResult(
                        cycle=cycle,
                        char_uuid=uuid,
                        success=False,
                        latency_ms=latency_ms,
                        bytes_read=0,
                        error=str(exc),
                    )
                )
                consecutive_fails += 1
                cycle_ok = False

                # Backoff on consecutive-fail streak
                if (
                    max_consecutive_fails > 0
                    and consecutive_fails >= max_consecutive_fails
                ):
                    backoffs_applied += 1
                    await _lprint(
                        f"  {prefix}⚠  {consecutive_fails} fallos consecutivos — "
                        f"backoff #{backoffs_applied}: esperando {backoff_s}s ...",
                        lock,
                    )
                    await asyncio.sleep(backoff_s)
                    consecutive_fails = 0

        completed_cycles += 1
        ok_count = sum(
            1 for r in reads if r.cycle == cycle and r.success
        )
        status = "✓" if cycle_ok else "✗"
        await _lprint(
            f"  {prefix}[{cycle:>3}/{n}] {status}  "
            f"OK: {ok_count}/{len(char_uuids)}  "
            + (
                f"Lat: {reads[-1].latency_ms:.1f}ms"
                if reads and reads[-1].latency_ms is not None
                else ""
            ),
            lock,
        )

        if cycle < n:
            await asyncio.sleep(delay_s)

    # ── Clean disconnect ──────────────────────────────────────────────────
    try:
        if client is not None and client.is_connected:
            await client.disconnect()
    except Exception:
        pass

    return reads, char_uuids, reconnects, backoffs_applied, completed_cycles


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

async def run_gatt_read_test(
    address: str,
    n: int = 20,
    delay_s: float = 0.5,
    connect_timeout: float = 10.0,
    read_timeout: float = 5.0,
    workers: int = 1,
    char_uuids: list[str] | None = None,
    max_consecutive_fails: int = 10,
    backoff_s: float = 5.0,
    verbose: bool = True,
    _label: str | None = None,
    _lock: asyncio.Lock | None = None,
) -> GattReadReport:
    """
    Runs N GATT read cycles against *address*.

    Args:
        address:               BLE device address / UUID.
        n:                     Read cycles per worker.
        delay_s:               Seconds between cycles.
        connect_timeout:       BleakClient connection timeout.
        read_timeout:          asyncio.wait_for timeout per individual GATT read.
        workers:               Number of independent concurrent connections.
        char_uuids:            UUIDs to read; None or [] = all readable characteristics.
        max_consecutive_fails: Trigger a backoff pause after this many consecutive
                               read failures across all characteristics. 0 = never.
        backoff_s:             Duration of the backoff pause.
        verbose:               Print per-cycle progress.
        _label:                Short label for multi-device runs (internal).
        _lock:                 Shared asyncio.Lock for synchronized output (internal).

    Returns:
        GattReadReport with full per-read breakdown.
    """
    workers = max(1, workers)
    char_filter: list[str] = list(char_uuids) if char_uuids else []
    prefix = f"[{_label}] " if _label else ""
    sep = "─" * 70

    if verbose:
        await _lprint(sep, _lock)
        await _lprint(f"{prefix}Test de lectura continua GATT (GATT Read Loop)", _lock)
        await _lprint(f"{prefix}Dispositivo  : {address}", _lock)
        await _lprint(
            f"{prefix}Ciclos: {n}  Workers: {workers}  Delay: {delay_s}s  "
            f"Read timeout: {read_timeout}s  Connect timeout: {connect_timeout}s",
            _lock,
        )
        if char_filter:
            await _lprint(f"{prefix}UUID filtradas: {', '.join(char_filter)}", _lock)
        if max_consecutive_fails > 0:
            await _lprint(
                f"{prefix}Backoff: pausa de {backoff_s}s tras "
                f"{max_consecutive_fails} fallos consecutivos",
                _lock,
            )
        await _lprint(sep, _lock)

    worker_label = (lambda i: f"{prefix}W{i + 1} ") if workers > 1 else (lambda _: prefix)

    worker_results: list[tuple[list[ReadResult], list[str], int, int, int]] = list(
        await asyncio.gather(
            *[
                _run_worker(
                    worker_id=i,
                    address=address,
                    n=n,
                    delay_s=delay_s,
                    connect_timeout=connect_timeout,
                    read_timeout=read_timeout,
                    char_uuids_filter=char_filter,
                    max_consecutive_fails=max_consecutive_fails,
                    backoff_s=backoff_s,
                    prefix=worker_label(i),
                    lock=_lock,
                )
                for i in range(workers)
            ]
        )
    )

    # ── Aggregate ─────────────────────────────────────────────────────────
    all_reads: list[ReadResult] = []
    total_reconnects = 0
    total_backoffs = 0
    total_completed = 0
    seen_uuids: list[str] = []

    for w_reads, w_uuids, w_reconnects, w_backoffs, w_completed in worker_results:
        all_reads.extend(w_reads)
        total_reconnects += w_reconnects
        total_backoffs += w_backoffs
        total_completed += w_completed
        for u in w_uuids:
            if u not in seen_uuids:
                seen_uuids.append(u)

    successful = [r for r in all_reads if r.success]
    latencies = [r.latency_ms for r in successful if r.latency_ms is not None]

    def _avg(lst: list[float]) -> float | None:
        return round(sum(lst) / len(lst), 2) if lst else None

    report = GattReadReport(
        address=address,
        char_uuids_tested=seen_uuids,
        total_cycles=n * workers,
        completed_cycles=total_completed,
        total_reads=len(all_reads),
        successful_reads=len(successful),
        failed_reads=len(all_reads) - len(successful),
        success_rate=len(successful) / len(all_reads) if all_reads else 0.0,
        min_latency_ms=round(min(latencies), 2) if latencies else None,
        max_latency_ms=round(max(latencies), 2) if latencies else None,
        avg_latency_ms=_avg(latencies),
        total_bytes_read=sum(r.bytes_read for r in successful),
        reconnects=total_reconnects,
        backoffs_applied=total_backoffs,
        workers=workers,
        reads=all_reads,
    )

    if verbose:
        if _lock is not None:
            async with _lock:
                _print_report(report, prefix=prefix)
        else:
            _print_report(report, prefix=prefix)

    return report


async def run_multi_gatt_read_test(
    configs: list[GattReadConfig],
    verbose: bool = True,
) -> list[GattReadReport]:
    """
    Runs run_gatt_read_test concurrently for each config in *configs*.
    All output is serialized through a shared asyncio.Lock.
    """
    lock = asyncio.Lock()
    tasks = [
        run_gatt_read_test(
            address=cfg.address,
            n=cfg.n,
            delay_s=cfg.delay_s,
            connect_timeout=cfg.connect_timeout,
            read_timeout=cfg.read_timeout,
            workers=cfg.workers,
            char_uuids=cfg.char_uuids or None,
            max_consecutive_fails=cfg.max_consecutive_fails,
            backoff_s=cfg.backoff_s,
            verbose=verbose,
            _label=cfg.name[:18],
            _lock=lock,
        )
        for cfg in configs
    ]
    reports: list[GattReadReport] = list(await asyncio.gather(*tasks))
    _print_multi_summary(reports)
    return reports


# ---------------------------------------------------------------------------
# Reporting
# ---------------------------------------------------------------------------

def _print_report(r: GattReadReport, prefix: str = "") -> None:
    sep = "─" * 70
    print(sep)
    print(f"{prefix}RESUMEN — {r.address}")
    print(sep)
    print(f"  {prefix}Workers              : {r.workers}")
    print(
        f"  {prefix}Ciclos completados   : {r.completed_cycles}/{r.total_cycles}"
    )
    print(
        f"  {prefix}Lecturas OK          : {r.successful_reads}/{r.total_reads}"
        f"  ({r.success_rate * 100:.1f}%)"
    )
    print(f"  {prefix}Lecturas fallidas    : {r.failed_reads}")
    print(f"  {prefix}Bytes leídos (total) : {r.total_bytes_read}")
    print(f"  {prefix}Reconexiones         : {r.reconnects}")
    print(f"  {prefix}Backoffs aplicados   : {r.backoffs_applied}")
    if r.avg_latency_ms is not None:
        print(f"  {prefix}Latencia mín         : {r.min_latency_ms:.2f} ms")
        print(f"  {prefix}Latencia máx         : {r.max_latency_ms:.2f} ms")
        print(f"  {prefix}Latencia prom        : {r.avg_latency_ms:.2f} ms")
    else:
        print(
            f"  {prefix}Latencia             : sin datos "
            "(todas las lecturas fallaron)"
        )
    if r.char_uuids_tested:
        print(f"  {prefix}Desglose por característica:")
        for uuid in r.char_uuids_tested:
            char_reads = [rd for rd in r.reads if rd.char_uuid == uuid]
            ok = sum(1 for rd in char_reads if rd.success)
            total = len(char_reads)
            latencies = [
                rd.latency_ms for rd in char_reads
                if rd.success and rd.latency_ms is not None
            ]
            avg_lat = (
                f"{round(sum(latencies) / len(latencies), 1):.1f}ms"
                if latencies else "—"
            )
            avg_bytes = (
                round(
                    sum(rd.bytes_read for rd in char_reads if rd.success) / ok, 1
                )
                if ok else 0
            )
            print(
                f"    {prefix}{uuid[:36]}  "
                f"{ok:>4}/{total:<4} OK  "
                f"~{avg_bytes:>5.1f}B/lectura  "
                f"lat prom {avg_lat}"
            )
    print(sep)


def _print_multi_summary(reports: list[GattReadReport]) -> None:
    sep = "─" * 70
    print()
    print(sep)
    print(" RESUMEN GLOBAL — Test de lectura continua GATT")
    print(sep)
    print(
        f"  {'Dispositivo':<36} {'Ciclos':>7}  {'Lecturas':>8}  "
        f"{'OK%':>6}  {'Lat prom':>9}  {'Bytes':>8}  {'W':>3}"
    )
    print("  " + "─" * 68)
    for r in reports:
        ok_pct = f"{r.success_rate * 100:.1f}%"
        lat = f"{r.avg_latency_ms:.1f}ms" if r.avg_latency_ms is not None else "—"
        print(
            f"  {r.address:<36} {r.completed_cycles:>7}  {r.total_reads:>8}  "
            f"{ok_pct:>6}  {lat:>9}  {r.total_bytes_read:>8}  {r.workers:>3}"
        )
    print(sep)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Test de lectura continua GATT (GATT Read Loop)",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "addresses", nargs="+", metavar="ADDRESS",
        help="BLE device address / UUID (se pueden pasar varios para test multi-dispositivo)",
    )
    parser.add_argument(
        "--n", type=int, default=20, metavar="N",
        help="Ciclos de lectura por worker",
    )
    parser.add_argument(
        "--delay", type=float, default=0.5, metavar="SEG",
        help="Segundos entre ciclos de lectura",
    )
    parser.add_argument(
        "--timeout", type=float, default=10.0, metavar="SEG",
        help="Timeout de conexión BLE",
    )
    parser.add_argument(
        "--read-timeout", type=float, default=5.0, metavar="SEG",
        help="Timeout por lectura GATT individual",
    )
    parser.add_argument(
        "--workers", type=int, default=1, metavar="W",
        help="Conexiones concurrentes independientes al mismo dispositivo",
    )
    parser.add_argument(
        "--chars", nargs="*", default=[], metavar="UUID",
        help="UUIDs de características a leer (omitir = todas las legibles)",
    )
    parser.add_argument(
        "--max-fails", type=int, default=10, metavar="N",
        help="Fallos consecutivos antes de aplicar backoff (0 = sin límite)",
    )
    parser.add_argument(
        "--backoff", type=float, default=5.0, metavar="SEG",
        help="Segundos de pausa tras alcanzar --max-fails",
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = _parse_args()
    if len(args.addresses) == 1:
        asyncio.run(
            run_gatt_read_test(
                address=args.addresses[0],
                n=args.n,
                delay_s=args.delay,
                connect_timeout=args.timeout,
                read_timeout=args.read_timeout,
                workers=args.workers,
                char_uuids=args.chars or None,
                max_consecutive_fails=args.max_fails,
                backoff_s=args.backoff,
            )
        )
    else:
        cfgs = [
            GattReadConfig(
                address=addr,
                name=addr[-17:],
                n=args.n,
                delay_s=args.delay,
                connect_timeout=args.timeout,
                read_timeout=args.read_timeout,
                workers=args.workers,
                char_uuids=args.chars or [],
                max_consecutive_fails=args.max_fails,
                backoff_s=args.backoff,
            )
            for addr in args.addresses
        ]
        asyncio.run(run_multi_gatt_read_test(cfgs))
