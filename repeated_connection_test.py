"""
repeated_connection_test.py — BLE repeated connection test.

Differs from stability_test in what it measures inside each connection:
  - Time to connect (connect latency)
  - Time the session stays alive (hold_s)
  - Services & characteristics discovered while connected
  - Whether the device disconnected unexpectedly before hold_s elapsed
  - Reconnect behaviour: does the device accept a new connection immediately?

Usage (standalone):
    python repeated_connection_test.py <address> [--n 10] [--hold 2.0]
                                                  [--delay 1.0] [--timeout 10.0]
                                                  [--workers 1]

Or called programmatically via run_repeated_test() / run_multi_repeated_test().
"""

import argparse
import asyncio
import time
from dataclasses import dataclass, field

from bleak import BleakClient
from bleak.exc import BleakError


# ---------------------------------------------------------------------------
# Data model
# ---------------------------------------------------------------------------

@dataclass
class SessionResult:
    attempt: int
    connected: bool               # reached connected state
    unexpected_drop: bool         # device dropped before hold_s ended
    connect_latency_ms: float | None
    session_duration_ms: float | None  # actual time held connected
    services_found: int           # number of GATT services discovered
    characteristics_found: int
    error: str | None


@dataclass
class RepeatedConnectionReport:
    address: str
    total: int
    connected_count: int
    unexpected_drops: int
    success_rate: float
    drop_rate: float
    min_latency_ms: float | None
    max_latency_ms: float | None
    avg_latency_ms: float | None
    avg_session_ms: float | None
    avg_services: float | None
    aborted_early: bool = False        # True if max_consecutive_fails was reached
    backoffs_applied: int = 0          # how many times backoff pauses were triggered
    workers: int = 1
    sessions: list[SessionResult] = field(default_factory=list)


@dataclass
class RepeatedConnectionConfig:
    """Per-device parameters for run_multi_repeated_test."""
    address: str
    name: str
    n: int
    hold_s: float
    delay_s: float
    connect_timeout: float
    workers: int
    max_consecutive_fails: int = 10    # 0 = never abort
    backoff_s: float = 5.0             # pause when consecutive fails hit threshold


# ---------------------------------------------------------------------------
# Core logic
# ---------------------------------------------------------------------------

async def _lprint(msg: str, lock: asyncio.Lock | None) -> None:
    if lock is not None:
        async with lock:
            print(msg, flush=True)
    else:
        print(msg, flush=True)


async def _single_session(
    address: str,
    hold_s: float,
    timeout: float,
) -> SessionResult:
    """
    Opens one BLE session, holds it for *hold_s* seconds, discovers services
    and records whether the device dropped the connection unexpectedly.
    """
    t_start = time.perf_counter()
    try:
        async with BleakClient(address, timeout=timeout) as client:
            if not client.is_connected:
                raise BleakError("Client reports not connected after context entry")

            connect_latency_ms = round((time.perf_counter() - t_start) * 1000, 2)

            # Discover GATT services
            services = client.services or []
            services_found = len(list(services))
            chars_found = sum(
                len(list(svc.characteristics)) for svc in services
            )

            # Hold the connection for hold_s, watching for unexpected drop
            t_hold = time.perf_counter()
            unexpected_drop = False
            try:
                await asyncio.sleep(hold_s)
                if not client.is_connected:
                    unexpected_drop = True
            except Exception:
                unexpected_drop = True

            session_duration_ms = round((time.perf_counter() - t_hold) * 1000, 2)

            return SessionResult(
                attempt=0,
                connected=True,
                unexpected_drop=unexpected_drop,
                connect_latency_ms=connect_latency_ms,
                session_duration_ms=session_duration_ms,
                services_found=services_found,
                characteristics_found=chars_found,
                error=None,
            )

    except (BleakError, asyncio.TimeoutError, OSError) as exc:
        return SessionResult(
            attempt=0,
            connected=False,
            unexpected_drop=False,
            connect_latency_ms=None,
            session_duration_ms=None,
            services_found=0,
            characteristics_found=0,
            error=str(exc),
        )


async def run_repeated_test(
    address: str,
    n: int = 10,
    hold_s: float = 2.0,
    delay_s: float = 1.0,
    connect_timeout: float = 10.0,
    workers: int = 1,
    max_consecutive_fails: int = 10,
    backoff_s: float = 5.0,
    verbose: bool = True,
    _label: str | None = None,
    _lock: asyncio.Lock | None = None,
) -> RepeatedConnectionReport:
    """
    Runs *n* connect → hold → disconnect cycles against *address*.

    Args:
        address:               BLE device address / UUID.
        n:                     Total connection cycles.
        hold_s:                Seconds to stay connected per cycle.
        delay_s:               Seconds between batches.
        connect_timeout:       Per-attempt BleakClient timeout.
        workers:               Concurrent connections per batch.
        max_consecutive_fails: Pause with backoff after this many consecutive
                               failures. 0 = keep retrying forever (no abort).
        backoff_s:             Seconds to wait during a backoff pause.
        verbose:               Print progress to stdout.
        _label:                Short label for multi-device output (internal).
        _lock:                 Shared asyncio.Lock for output (internal).

    Returns:
        RepeatedConnectionReport with full per-session breakdown.
    """
    workers = max(1, workers)
    prefix = f"[{_label}] " if _label else ""
    sep = "─" * 70

    if verbose:
        await _lprint(sep, _lock)
        await _lprint(f"{prefix}Test de conexión repetida BLE", _lock)
        await _lprint(f"{prefix}Dispositivo : {address}", _lock)
        await _lprint(
            f"{prefix}Ciclos: {n}  Hold: {hold_s}s  Workers: {workers}  "
            f"Delay: {delay_s}s  Timeout: {connect_timeout}s",
            _lock,
        )
        if max_consecutive_fails > 0:
            await _lprint(
                f"{prefix}Backoff: pausa de {backoff_s}s tras {max_consecutive_fails} "
                f"fallos consecutivos",
                _lock,
            )
        await _lprint(sep, _lock)

    sessions: list[SessionResult] = []
    attempt_num = 0
    remaining = n
    consecutive_fails = 0
    backoffs_applied = 0
    aborted_early = False

    while remaining > 0:
        batch_size = min(workers, remaining)
        batch: list[SessionResult] = list(await asyncio.gather(
            *[_single_session(address, hold_s, connect_timeout) for _ in range(batch_size)]
        ))
        for result in batch:
            attempt_num += 1
            result.attempt = attempt_num

            if result.connected:
                consecutive_fails = 0
            else:
                consecutive_fails += 1

            if verbose:
                if result.connected:
                    drop_tag = " [DROP]" if result.unexpected_drop else ""
                    await _lprint(
                        f"  {prefix}[{attempt_num:>3}/{n}] ✓  "
                        f"Latencia: {result.connect_latency_ms:>8.2f} ms  "
                        f"Sesión: {result.session_duration_ms:>8.2f} ms  "
                        f"Servicios: {result.services_found}  "
                        f"Chars: {result.characteristics_found}{drop_tag}",
                        _lock,
                    )
                else:
                    await _lprint(
                        f"  {prefix}[{attempt_num:>3}/{n}] ✗  Error: {result.error}",
                        _lock,
                    )
            sessions.append(result)

        remaining -= batch_size

        # ── Backoff / abort on consecutive failure streak ────────────────────────
        if max_consecutive_fails > 0 and consecutive_fails >= max_consecutive_fails:
            if remaining <= 0:
                break  # already done, no need to pause
            backoffs_applied += 1
            await _lprint(
                f"  {prefix}⚠̃  {consecutive_fails} fallos consecutivos — "
                f"backoff #{backoffs_applied}: esperando {backoff_s}s ...",
                _lock,
            )
            await asyncio.sleep(backoff_s)
            consecutive_fails = 0  # reset counter; try again after pause

        elif remaining > 0:
            await asyncio.sleep(delay_s)

    # Aggregate
    connected  = [s for s in sessions if s.connected]
    drops      = [s for s in connected if s.unexpected_drop]
    latencies  = [s.connect_latency_ms for s in connected if s.connect_latency_ms is not None]
    durations  = [s.session_duration_ms for s in connected if s.session_duration_ms is not None]
    svc_counts = [s.services_found for s in connected]

    def _avg(lst):
        return round(sum(lst) / len(lst), 2) if lst else None

    report = RepeatedConnectionReport(
        address=address,
        total=attempt_num,   # may be < n if aborted early (future use)
        connected_count=len(connected),
        unexpected_drops=len(drops),
        success_rate=len(connected) / attempt_num if attempt_num else 0.0,
        drop_rate=len(drops) / len(connected) if connected else 0.0,
        min_latency_ms=round(min(latencies), 2) if latencies else None,
        max_latency_ms=round(max(latencies), 2) if latencies else None,
        avg_latency_ms=_avg(latencies),
        avg_session_ms=_avg(durations),
        avg_services=_avg(svc_counts),
        aborted_early=aborted_early,
        backoffs_applied=backoffs_applied,
        workers=workers,
        sessions=sessions,
    )

    if verbose:
        if _lock is not None:
            async with _lock:
                _print_report(report, prefix=prefix)
        else:
            _print_report(report, prefix=prefix)

    return report


def _print_report(r: RepeatedConnectionReport, prefix: str = "") -> None:
    sep = "─" * 70
    print(sep)
    print(f"{prefix}RESUMEN — {r.address}")
    print(sep)
    print(f"  {prefix}Workers           : {r.workers}")
    print(f"  {prefix}Conectados        : {r.connected_count}/{r.total}  ({r.success_rate * 100:.1f}%)")
    print(f"  {prefix}Drops inesperados : {r.unexpected_drops}  ({r.drop_rate * 100:.1f}% de conexiones)")
    print(f"  {prefix}Backoffs aplicados: {r.backoffs_applied}")
    if r.avg_latency_ms is not None:
        print(f"  {prefix}Latencia mín      : {r.min_latency_ms:.2f} ms")
        print(f"  {prefix}Latencia máx      : {r.max_latency_ms:.2f} ms")
        print(f"  {prefix}Latencia prom     : {r.avg_latency_ms:.2f} ms")
    else:
        print(f"  {prefix}Latencia          : sin datos (todas las conexiones fallaron)")
    if r.avg_session_ms is not None:
        print(f"  {prefix}Sesión prom       : {r.avg_session_ms:.2f} ms")
    if r.avg_services is not None:
        print(f"  {prefix}Servicios prom    : {r.avg_services:.1f}")
    print(sep)


# ---------------------------------------------------------------------------
# Multi-device concurrent test
# ---------------------------------------------------------------------------

async def run_multi_repeated_test(
    configs: list[RepeatedConnectionConfig],
    verbose: bool = True,
) -> list[RepeatedConnectionReport]:
    """
    Runs repeated connection tests on multiple devices fully concurrently.
    Each device uses its own RepeatedConnectionConfig.

    Returns:
        List of RepeatedConnectionReport in submission order.
    """
    if not configs:
        return []

    lock = asyncio.Lock()

    if verbose:
        sep  = "═" * 92
        sep2 = "─" * 92
        print(sep)
        print(" Test de conexión repetida BLE — MULTI-DISPOSITIVO")
        print(sep)
        print(
            f"  {'Dispositivo':<34} {'Nombre':<22} {'n':>4}  "
            f"{'W':>3}  {'Hold':>6}  {'Delay':>6}  {'Timeout':>7}"
        )
        print(sep2)
        for cfg in configs:
            print(
                f"  {cfg.address:<34} {cfg.name:<22} {cfg.n:>4}  "
                f"{cfg.workers:>3}  {cfg.hold_s:>5.1f}s  {cfg.delay_s:>5.1f}s  "
                f"{cfg.connect_timeout:>6.1f}s"
            )
        print(sep)

    tasks = [
        run_repeated_test(
            address=cfg.address,
            n=cfg.n,
            hold_s=cfg.hold_s,
            delay_s=cfg.delay_s,
            connect_timeout=cfg.connect_timeout,
            workers=cfg.workers,
            max_consecutive_fails=cfg.max_consecutive_fails,
            backoff_s=cfg.backoff_s,
            verbose=verbose,
            _label=cfg.name[:18],
            _lock=lock,
        )
        for cfg in configs
    ]

    reports: list[RepeatedConnectionReport] = list(await asyncio.gather(*tasks))

    if verbose:
        _print_multi_summary(reports)

    return reports


def _print_multi_summary(reports: list[RepeatedConnectionReport]) -> None:
    sep  = "═" * 100
    sep2 = "─" * 100
    print()
    print(sep)
    print("  RESUMEN GLOBAL — TEST DE CONEXIÓN REPETIDA MULTI-DISPOSITIVO")
    print(sep)
    print(
        f"  {'Dispositivo':<42} {'W':>3} {'OK':>4} {'DROP':>5} {'Tasa':>6}  "
        f"{'LatProm':>8}  {'SesProm':>8}  {'Svcs':>5}"
    )
    print(sep2)
    for r in reports:
        lat  = f"{r.avg_latency_ms:.0f}"  if r.avg_latency_ms  is not None else "N/A"
        ses  = f"{r.avg_session_ms:.0f}"  if r.avg_session_ms  is not None else "N/A"
        svcs = f"{r.avg_services:.1f}"    if r.avg_services     is not None else "N/A"
        print(
            f"  {r.address:<42} {r.workers:>3} {r.connected_count:>4} "
            f"{r.unexpected_drops:>5} {r.success_rate * 100:>5.1f}%  "
            f"{lat:>8}  {ses:>8}  {svcs:>5}"
        )
    print(sep)


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------

def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Test de conexión repetida BLE (uno o múltiples dispositivos)",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "addresses", nargs="+", metavar="ADDRESS",
        help="Dirección(es) BLE. Pasa varias para test multi-dispositivo.",
    )
    parser.add_argument("--n", type=int, default=10, metavar="N",
                        help="Ciclos de conexión por dispositivo")
    parser.add_argument("--hold", type=float, default=2.0, metavar="SEG",
                        help="Segundos a mantener la conexión activa por ciclo")
    parser.add_argument("--delay", type=float, default=1.0, metavar="SEG",
                        help="Segundos de espera entre lotes")
    parser.add_argument("--timeout", type=float, default=10.0, metavar="SEG",
                        help="Timeout de conexión por intento")
    parser.add_argument("--workers", type=int, default=1, metavar="W",
                        help="Conexiones concurrentes por dispositivo por lote")
    parser.add_argument("--max-fails", type=int, default=10, metavar="N",
                        help="Fallos consecutivos antes de aplicar backoff (0 = sin límite)")
    parser.add_argument("--backoff", type=float, default=5.0, metavar="SEG",
                        help="Segundos de pausa tras alcanzar --max-fails")
    return parser.parse_args()


if __name__ == "__main__":
    args = _parse_args()
    if len(args.addresses) == 1:
        asyncio.run(
            run_repeated_test(
                address=args.addresses[0],
                n=args.n,
                hold_s=args.hold,
                delay_s=args.delay,
                connect_timeout=args.timeout,
                workers=args.workers,
                max_consecutive_fails=args.max_fails,
                backoff_s=args.backoff,
            )
        )
    else:
        cfgs = [
            RepeatedConnectionConfig(
                address=addr,
                name=addr[-17:],
                n=args.n,
                hold_s=args.hold,
                delay_s=args.delay,
                connect_timeout=args.timeout,
                workers=args.workers,
                max_consecutive_fails=args.max_fails,
                backoff_s=args.backoff,
            )
            for addr in args.addresses
        ]
        asyncio.run(run_multi_repeated_test(cfgs))
