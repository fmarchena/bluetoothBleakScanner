"""
stability_test.py — BLE connection stability tester.

Usage (standalone):
    python stability_test.py <address> [--n 20] [--delay 1.0] [--timeout 10.0]

Or called programmatically via run_stability_test().
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
class AttemptResult:
    attempt: int
    success: bool
    latency_ms: float | None   # time to connect, None on failure
    error: str | None          # error message, None on success


@dataclass
class StabilityReport:
    address: str
    total: int
    successes: int
    failures: int
    success_rate: float           # 0.0 – 1.0
    min_latency_ms: float | None
    max_latency_ms: float | None
    avg_latency_ms: float | None
    workers: int = 1
    attempts: list[AttemptResult] = field(default_factory=list)


@dataclass
class DeviceTestConfig:
    """Per-device parameters for run_multi_stability_test."""
    address: str
    name: str
    n: int
    delay_s: float
    connect_timeout: float
    workers: int


# ---------------------------------------------------------------------------
# Core logic
# ---------------------------------------------------------------------------

async def _single_connect(address: str, timeout: float) -> AttemptResult:
    """Performs one connect → disconnect cycle and returns the result."""
    t0 = time.perf_counter()
    try:
        async with BleakClient(address, timeout=timeout) as client:
            if not client.is_connected:
                raise BleakError("Client reports not connected after context entry")
            latency_ms = (time.perf_counter() - t0) * 1000
            return AttemptResult(
                attempt=0,          # filled by caller
                success=True,
                latency_ms=round(latency_ms, 2),
                error=None,
            )
    except (BleakError, asyncio.TimeoutError, OSError) as exc:
        return AttemptResult(
            attempt=0,
            success=False,
            latency_ms=None,
            error=str(exc),
        )


async def _lprint(msg: str, lock: asyncio.Lock | None) -> None:
    """Prints *msg* atomically; acquires *lock* first when in multi-device mode."""
    if lock is not None:
        async with lock:
            print(msg, flush=True)
    else:
        print(msg, flush=True)


async def run_stability_test(
    address: str,
    n: int = 10,
    delay_s: float = 1.0,
    connect_timeout: float = 10.0,
    workers: int = 1,
    verbose: bool = True,
    _label: str | None = None,
    _lock: asyncio.Lock | None = None,
) -> StabilityReport:
    """
    Connects and disconnects to *address* up to *n* times.

    Args:
        address:         BLE device address / UUID.
        n:               Total connect/disconnect cycles.
        delay_s:         Seconds to wait between batches.
        connect_timeout: Per-attempt BleakClient timeout in seconds.
        workers:         Concurrent BleakClient connections per batch against this device.
        verbose:         Print progress to stdout.
        _label:          Short label prefix for multi-device output (internal).
        _lock:           Shared asyncio.Lock for synchronized output (internal).

    Returns:
        StabilityReport with full per-attempt breakdown.
    """
    workers = max(1, workers)
    prefix = f"[{_label}] " if _label else ""
    sep = "─" * 70

    if verbose:
        await _lprint(sep, _lock)
        await _lprint(f"{prefix}Test de estabilidad BLE", _lock)
        await _lprint(f"{prefix}Dispositivo : {address}", _lock)
        await _lprint(
            f"{prefix}Intentos: {n}  Workers: {workers}  Delay: {delay_s}s  Timeout: {connect_timeout}s",
            _lock,
        )
        await _lprint(sep, _lock)

    results: list[AttemptResult] = []
    attempt_num = 0
    remaining = n

    while remaining > 0:
        batch_size = min(workers, remaining)
        batch_results: tuple[AttemptResult, ...] = await asyncio.gather(
            *[_single_connect(address, connect_timeout) for _ in range(batch_size)]
        )
        for result in batch_results:
            attempt_num += 1
            result.attempt = attempt_num
            if verbose:
                if result.success:
                    await _lprint(
                        f"  {prefix}[{attempt_num:>3}/{n}] ✓  Latencia: {result.latency_ms:>8.2f} ms",
                        _lock,
                    )
                else:
                    await _lprint(
                        f"  {prefix}[{attempt_num:>3}/{n}] ✗  Error: {result.error}",
                        _lock,
                    )
            results.append(result)
        remaining -= batch_size
        if remaining > 0:
            await asyncio.sleep(delay_s)

    # Build summary
    successes = [r for r in results if r.success]
    failures  = [r for r in results if not r.success]
    latencies = [r.latency_ms for r in successes if r.latency_ms is not None]

    report = StabilityReport(
        address=address,
        total=n,
        successes=len(successes),
        failures=len(failures),
        success_rate=len(successes) / n,
        min_latency_ms=round(min(latencies), 2) if latencies else None,
        max_latency_ms=round(max(latencies), 2) if latencies else None,
        avg_latency_ms=round(sum(latencies) / len(latencies), 2) if latencies else None,
        workers=workers,
        attempts=results,
    )

    if verbose:
        if _lock is not None:
            async with _lock:
                _print_report(report, prefix=prefix)
        else:
            _print_report(report, prefix=prefix)

    return report


def _print_report(r: StabilityReport, prefix: str = "") -> None:
    sep = "─" * 70
    print(sep)
    print(f"{prefix}RESUMEN — {r.address}")
    print(sep)
    print(f"  {prefix}Workers       : {r.workers}")
    print(f"  {prefix}Exitosos      : {r.successes}/{r.total}  ({r.success_rate * 100:.1f}%)")
    print(f"  {prefix}Fallidos      : {r.failures}/{r.total}")
    if r.avg_latency_ms is not None:
        print(f"  {prefix}Latencia mín  : {r.min_latency_ms:.2f} ms")
        print(f"  {prefix}Latencia máx  : {r.max_latency_ms:.2f} ms")
        print(f"  {prefix}Latencia prom : {r.avg_latency_ms:.2f} ms")
    else:
        print(f"  {prefix}Latencia      : sin datos (todas las conexiones fallaron)")
    print(sep)


# ---------------------------------------------------------------------------
# Multi-device concurrent test
# ---------------------------------------------------------------------------

async def run_multi_stability_test(
    configs: list[DeviceTestConfig],
    verbose: bool = True,
) -> list[StabilityReport]:
    """
    Runs stability tests on multiple devices fully concurrently.

    Each device uses its own DeviceTestConfig (n, delay_s, connect_timeout, workers).
    All devices start simultaneously; concurrency within each is controlled by
    its own *workers* value.

    Returns:
        List of StabilityReport in submission order.
    """
    if not configs:
        return []

    lock = asyncio.Lock()

    if verbose:
        sep = "═" * 84
        sep2 = "─" * 84
        print(sep)
        print(" Test de estabilidad BLE — MULTI-DISPOSITIVO")
        print(sep)
        print(f"  {'Dispositivo':<34} {'Nombre':<22} {'n':>4}  {'W':>3}  {'Delay':>6}  {'Timeout':>7}")
        print(sep2)
        for cfg in configs:
            print(
                f"  {cfg.address:<34} {cfg.name:<22} {cfg.n:>4}  "
                f"{cfg.workers:>3}  {cfg.delay_s:>5.1f}s  {cfg.connect_timeout:>6.1f}s"
            )
        print(sep)

    tasks = [
        run_stability_test(
            address=cfg.address,
            n=cfg.n,
            delay_s=cfg.delay_s,
            connect_timeout=cfg.connect_timeout,
            workers=cfg.workers,
            verbose=verbose,
            _label=cfg.name[:18],
            _lock=lock,
        )
        for cfg in configs
    ]

    reports: list[StabilityReport] = list(await asyncio.gather(*tasks))

    if verbose:
        _print_multi_summary(reports)

    return reports


def _print_multi_summary(reports: list[StabilityReport]) -> None:
    """Prints a consolidated comparison table for all tested devices."""
    sep  = "═" * 92
    sep2 = "─" * 92
    print()
    print(sep)
    print("  RESUMEN GLOBAL — TEST MULTI-DISPOSITIVO")
    print(sep)
    print(
        f"  {'Dispositivo':<42} {'W':>3} {'OK':>4} {'FAIL':>4} {'Tasa':>6}  "
        f"{'Prom ms':>8}  {'Mín ms':>7}  {'Máx ms':>7}"
    )
    print(sep2)
    for r in reports:
        avg = f"{r.avg_latency_ms:.0f}" if r.avg_latency_ms is not None else "N/A"
        mn  = f"{r.min_latency_ms:.0f}" if r.min_latency_ms is not None else "N/A"
        mx  = f"{r.max_latency_ms:.0f}" if r.max_latency_ms is not None else "N/A"
        print(
            f"  {r.address:<42} {r.workers:>3} {r.successes:>4} {r.failures:>4} "
            f"{r.success_rate * 100:>5.1f}%  {avg:>8}  {mn:>7}  {mx:>7}"
        )
    print(sep)


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------

def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Test de estabilidad de conexión BLE (uno o múltiples dispositivos)",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "addresses", nargs="+", metavar="ADDRESS",
        help="Dirección(es) BLE. Pasa varias para test multi-dispositivo simultáneo.",
    )
    parser.add_argument("--n", type=int, default=10, metavar="N",
                        help="Ciclos conectar/desconectar por dispositivo")
    parser.add_argument("--delay", type=float, default=1.0, metavar="SEG",
                        help="Segundos de espera entre intentos")
    parser.add_argument("--timeout", type=float, default=10.0, metavar="SEG",
                        help="Timeout de conexión por intento")
    parser.add_argument("--workers", type=int, default=3, metavar="W",
                        help="Máximo de dispositivos bajo test simultáneamente")
    return parser.parse_args()


if __name__ == "__main__":
    args = _parse_args()
    if len(args.addresses) == 1:
        asyncio.run(
            run_stability_test(
                address=args.addresses[0],
                n=args.n,
                delay_s=args.delay,
                connect_timeout=args.timeout,
                workers=args.workers,
            )
        )
    else:
        # Build uniform configs using global params for all addresses
        cfgs = [
            DeviceTestConfig(
                address=addr,
                name=addr[-17:],
                n=args.n,
                delay_s=args.delay,
                connect_timeout=args.timeout,
                workers=args.workers,
            )
            for addr in args.addresses
        ]
        asyncio.run(run_multi_stability_test(cfgs))
