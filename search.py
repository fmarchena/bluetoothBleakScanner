"""
Entry point — BLE scanner with persistent SQLite tracking.

Modes
-----
scan (default)
    Scans for BLE devices, persists results and prints a summary.

--test
    After the scan, runs a connection stability test
    (N × connect/disconnect cycles, per-device configurable).

--repeat-test
    After the scan, runs a repeated connection test
    (N × connect → hold → disconnect, measuring session duration,
    GATT services discovered, unexpected drops and reconnect behaviour).

Examples
--------
    python search.py
    python search.py --test
    python search.py --test   --n 30 --delay 0.5 --timeout 15 --workers 3
    python search.py --repeat-test
    python search.py --repeat-test --n 10 --hold 3.0 --delay 1.0 --timeout 15
"""

import argparse
import asyncio
import json
from dataclasses import dataclass

from db import DB_PATH, DeviceRecord, get_connection, init_schema, insert_sighting, upsert_device
from scanner import build_metadata, extract_manufacturer_name, scan_devices
from stability_test import DeviceTestConfig, run_multi_stability_test, run_stability_test
from repeated_connection_test import (
    RepeatedConnectionConfig,
    run_multi_repeated_test,
    run_repeated_test,
)
from gatt_read_test import (
    GattReadConfig,
    run_gatt_read_test,
    run_multi_gatt_read_test,
)
from utils import to_json_safe, utc_now_iso


@dataclass(frozen=True, slots=True)
class ScannedDevice:
    id: str
    nombre: str
    rssi: int | None
    rssi_avg: float | None
    fabricante: str | None
    servicios: list[str]
    first_seen: str
    last_seen: str
    es_nuevo: bool
    total_veces: int


def persist_entries(
    conn,
    entries: list[tuple],
) -> tuple[list[ScannedDevice], int]:
    """Persists every scan entry to the DB. Returns (processed_list, new_count)."""
    procesados: list[ScannedDevice] = []
    nuevos = 0

    for device, adv in entries:
        device_id = getattr(device, "address", None) or "sin_id"
        nombre = getattr(device, "name", None) or "sin_nombre"
        seen_at = utc_now_iso()
        rssi = getattr(adv, "rssi", None)
        manufacturer_data = getattr(adv, "manufacturer_data", None)
        service_uuids: list[str] = list(getattr(adv, "service_uuids", None) or [])
        fabricante = extract_manufacturer_name(manufacturer_data)

        rec: DeviceRecord = upsert_device(
            conn,
            device_id=device_id,
            name=nombre,
            seen_at=seen_at,
            rssi=rssi,
            manufacturer_name=fabricante,
            service_uuids_json=json.dumps(
                to_json_safe(service_uuids), ensure_ascii=True
            ),
            manufacturer_data_json=json.dumps(
                to_json_safe(manufacturer_data), ensure_ascii=True
            ),
            service_data_json=json.dumps(
                to_json_safe(getattr(adv, "service_data", None)), ensure_ascii=True
            ),
            tx_power=getattr(adv, "tx_power", None),
            platform_data_json=json.dumps(
                to_json_safe(getattr(adv, "platform_data", None)), ensure_ascii=True
            ),
        )
        insert_sighting(
            conn,
            device_id=device_id,
            seen_at=seen_at,
            name=nombre,
            rssi=rssi,
            metadata_json=json.dumps(build_metadata(device, adv), ensure_ascii=True),
        )

        if rec.is_new:
            nuevos += 1

        procesados.append(
            ScannedDevice(
                id=device_id,
                nombre=nombre,
                rssi=rssi,
                rssi_avg=rec.rssi_avg,
                fabricante=fabricante,
                servicios=service_uuids,
                first_seen=rec.first_seen,
                last_seen=rec.last_seen,
                es_nuevo=rec.is_new,
                total_veces=rec.total,
            )
        )

    return procesados, nuevos


def print_results(procesados: list[ScannedDevice], nuevos: int) -> None:
    """Prints a formatted summary of the scan results to stdout."""
    sep = "─" * 80
    print(f"Base de datos : {DB_PATH}")
    print(f"Detectados    : {len(procesados)}")
    print(f"Nuevos        : {nuevos}")
    print(f"Conocidos     : {len(procesados) - nuevos}")
    print()
    for i, item in enumerate(procesados, start=1):
        estado = "[ NUEVO  ]" if item.es_nuevo else "[conocido]"
        rssi_avg_str = f"{item.rssi_avg:.1f} dBm" if item.rssi_avg is not None else "N/A"
        servicios_str = ", ".join(item.servicios) if item.servicios else "—"
        print(sep)
        print(
            f"[{i:>3}] {estado}  ID: {item.id}  Nombre: {item.nombre}\n"
            f"       Fabricante    : {item.fabricante or '—'}\n"
            f"       RSSI actual   : {str(item.rssi):>5} dBm   RSSI promedio: {rssi_avg_str}\n"
            f"       1ª detección  : {item.first_seen}\n"
            f"       Última vista  : {item.last_seen}\n"
            f"       Servicios BLE : {servicios_str}\n"
            f"       Apariciones   : {item.total_veces}"
        )
    print(sep)


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="BLE scanner con seguimiento SQLite",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "--test", action="store_true",
        help="Tras el escaneo, selecciona un dispositivo y ejecuta el test de estabilidad",
    )
    parser.add_argument(
        "--n", type=int, default=10, metavar="N",
        help="Ciclos conectar/desconectar para el test de estabilidad",
    )
    parser.add_argument(
        "--delay", type=float, default=1.0, metavar="SEG",
        help="Segundos de espera entre intentos",
    )
    parser.add_argument(
        "--timeout", type=float, default=10.0, metavar="SEG",
        help="Timeout de conexión por intento",
    )
    parser.add_argument(
        "--workers", type=int, default=3, metavar="W",
        help="Dispositivos bajo test simultáneamente (con --test y múltiples seleccionados)",
    )
    # ── repeated-connection test flags ──────────────────────────────────────
    parser.add_argument(
        "--repeat-test", dest="repeat_test", action="store_true",
        help="Tras el escaneo, ejecuta el test de conexión repetida",
    )
    parser.add_argument(
        "--hold", type=float, default=2.0, metavar="SEG",
        help="Segundos a mantener la conexión activa por ciclo (--repeat-test)",
    )
    parser.add_argument(
        "--max-fails", dest="max_fails", type=int, default=10, metavar="N",
        help="Fallos consecutivos que activan un backoff (0 = sin límite)",
    )
    parser.add_argument(
        "--backoff", type=float, default=5.0, metavar="SEG",
        help="Segundos de pausa tras alcanzar --max-fails",
    )
    # ── gatt-read-test flags ─────────────────────────────────────────────
    parser.add_argument(
        "--gatt-test", dest="gatt_test", action="store_true",
        help="Tras el escaneo, ejecuta el test de lectura continua GATT",
    )
    parser.add_argument(
        "--read-timeout", dest="read_timeout", type=float, default=5.0, metavar="SEG",
        help="Timeout por lectura GATT individual (--gatt-test)",
    )
    parser.add_argument(
        "--chars", nargs="*", default=[], metavar="UUID",
        help="UUIDs de características GATT a leer (vacío = todas las legibles)",
    )
    return parser.parse_args()


def _pick_devices(procesados: list[ScannedDevice]) -> list[ScannedDevice]:
    """
    Shows a numbered list and returns the devices chosen by the user.

    Accepted input formats:
      5          → device 5
      1,3,5      → devices 1, 3 and 5
      1-5        → devices 1 through 5
      * or all   → all devices
      0          → cancel
    """
    sep = "─" * 70
    print()
    print(sep)
    print(" Selecciona dispositivo(s) para el test de estabilidad")
    print(" Formatos: 5  |  1,3,5  |  1-5  |  * ó all  |  0 para cancelar")
    print(sep)
    for i, d in enumerate(procesados, start=1):
        print(f"  [{i:>3}] {d.id:<42}  {d.nombre}")
    print(sep)

    total = len(procesados)
    while True:
        raw = input("  Selección > ").strip().lower()
        if raw == "0":
            return []
        if raw in ("*", "all", "todos"):
            return list(procesados)

        indices: set[int] = set()
        valid = True
        for part in raw.split(","):
            part = part.strip()
            if "-" in part:
                bounds = part.split("-", 1)
                if all(b.isdigit() for b in bounds):
                    lo, hi = int(bounds[0]), int(bounds[1])
                    if 1 <= lo <= hi <= total:
                        indices.update(range(lo, hi + 1))
                    else:
                        print(f"  Rango fuera de límites: {part} (1–{total})")
                        valid = False
                        break
                else:
                    valid = False
                    break
            elif part.isdigit():
                idx = int(part)
                if 1 <= idx <= total:
                    indices.add(idx)
                else:
                    print(f"  Número fuera de rango: {idx} (1–{total})")
                    valid = False
                    break
            else:
                valid = False
                break

        if not valid or not indices:
            print("  Entrada no válida. Ejemplos: 1  |  1,3  |  2-5  |  *")
            continue

        selected = [procesados[i - 1] for i in sorted(indices)]
        print(f"  Seleccionados ({len(selected)}): {', '.join(d.nombre for d in selected)}")
        return selected


def _configure_devices(
    selected: list[ScannedDevice],
    default_n: int,
    default_delay: float,
    default_timeout: float,
    default_workers: int,
) -> list[DeviceTestConfig]:
    """
    Prompts per-device test parameters.
    Pressing Enter on any field keeps the shown global default.
    """
    sep = "─" * 70

    def _ask(prompt: str, default, cast):
        while True:
            raw = input(f"      {prompt:<18} [{default}] > ").strip()
            if not raw:
                return default
            try:
                val = cast(raw)
                if val <= 0:
                    raise ValueError
                return val
            except ValueError:
                print(f"      Valor inválido — ingresa un número positivo.")

    configs: list[DeviceTestConfig] = []
    print()
    print(sep)
    print(" Configuración por dispositivo  (Enter = mantener valor global)")
    print(sep)

    for d in selected:
        print(f"\n  [{d.nombre}]  {d.id}")
        n        = _ask("n  (intentos) ", default_n,       int)
        delay    = _ask("delay  (seg)  ", default_delay,   float)
        timeout  = _ask("timeout (seg) ", default_timeout, float)
        workers  = _ask("workers       ", default_workers,  int)
        configs.append(
            DeviceTestConfig(
                address=d.id,
                name=d.nombre,
                n=n,
                delay_s=delay,
                connect_timeout=timeout,
                workers=workers,
            )
        )

    print()
    print(sep)
    print(" Resumen de configuración")
    print(sep)
    print(f"  {'Dispositivo':<34} {'Nombre':<22} {'n':>4}  {'W':>3}  {'Delay':>6}  {'Timeout':>7}")
    print("  " + "─" * 68)
    for cfg in configs:
        print(
            f"  {cfg.address:<34} {cfg.name:<22} {cfg.n:>4}  "
            f"{cfg.workers:>3}  {cfg.delay_s:>5.1f}s  {cfg.connect_timeout:>6.1f}s"
        )
    print(sep)
    while True:
        confirm = input("  ¿Iniciar test? [s/N] > ").strip().lower()
        if confirm in ("s", "si", "sí", "y", "yes"):
            return configs
        if confirm in ("", "n", "no"):
            return []


def _configure_repeated_devices(
    selected: list[ScannedDevice],
    default_n: int,
    default_hold: float,
    default_delay: float,
    default_timeout: float,
    default_workers: int,
    default_max_fails: int = 10,
    default_backoff: float = 5.0,
) -> list[RepeatedConnectionConfig]:
    """
    Prompts per-device parameters for the repeated connection test.
    Pressing Enter keeps the shown global default.
    """
    sep = "─" * 70

    def _ask(prompt: str, default, cast):
        while True:
            raw = input(f"      {prompt:<20} [{default}] > ").strip()
            if not raw:
                return default
            try:
                val = cast(raw)
                if val <= 0:
                    raise ValueError
                return val
            except ValueError:
                print("      Valor inválido — ingresa un número positivo.")

    configs: list[RepeatedConnectionConfig] = []
    print()
    print(sep)
    print(" Configuración por dispositivo — Test de conexión repetida")
    print(" (Enter = mantener valor global)")
    print(sep)

    for d in selected:
        print(f"\n  [{d.nombre}]  {d.id}")
        n       = _ask("n  (ciclos)   ",  default_n,       int)
        hold    = _ask("hold  (seg)   ",  default_hold,    float)
        delay   = _ask("delay  (seg)  ",  default_delay,   float)
        timeout = _ask("timeout (seg) ",  default_timeout, float)
        workers = _ask("workers       ",  default_workers,  int)
        max_f   = _ask("max-fails     ",  default_max_fails, int)
        backoff = _ask("backoff (seg) ",  default_backoff,  float)
        configs.append(
            RepeatedConnectionConfig(
                address=d.id,
                name=d.nombre,
                n=n,
                hold_s=hold,
                delay_s=delay,
                connect_timeout=timeout,
                workers=workers,
                max_consecutive_fails=max_f,
                backoff_s=backoff,
            )
        )

    print()
    print(sep)
    print(" Resumen de configuración")
    print(sep)
    print(
        f"  {'Dispositivo':<34} {'Nombre':<22} {'n':>4}  "
        f"{'W':>3}  {'Hold':>6}  {'Delay':>6}  {'Timeout':>7}"
    )
    print("  " + "─" * 72)
    for cfg in configs:
        print(
            f"  {cfg.address:<34} {cfg.name:<22} {cfg.n:>4}  "
            f"{cfg.workers:>3}  {cfg.hold_s:>5.1f}s  {cfg.delay_s:>5.1f}s  "
            f"{cfg.connect_timeout:>6.1f}s"
        )
    print(sep)
    while True:
        confirm = input("  ¿Iniciar test? [s/N] > ").strip().lower()
        if confirm in ("s", "si", "sí", "y", "yes"):
            return configs
        if confirm in ("", "n", "no"):
            return []


def _configure_gatt_devices(
    selected: list[ScannedDevice],
    default_n: int,
    default_delay: float,
    default_timeout: float,
    default_read_timeout: float,
    default_workers: int,
    default_max_fails: int = 10,
    default_backoff: float = 5.0,
    default_chars: list[str] | None = None,
) -> list[GattReadConfig]:
    """
    Prompts per-device parameters for the GATT read loop test.
    Pressing Enter keeps the shown global default.
    """
    sep = "─" * 70

    def _ask(prompt: str, default, cast, allow_zero: bool = False):
        while True:
            raw = input(f"      {prompt:<22} [{default}] > ").strip()
            if not raw:
                return default
            try:
                val = cast(raw)
                if not allow_zero and val <= 0:
                    raise ValueError
                if allow_zero and val < 0:
                    raise ValueError
                return val
            except ValueError:
                print("      Valor inválido — ingresa un número positivo.")

    configs: list[GattReadConfig] = []
    print()
    print(sep)
    print(" Configuración por dispositivo — Test de lectura continua GATT")
    print(" (Enter = mantener valor global)")
    print(sep)

    for d in selected:
        print(f"\n  [{d.nombre}]  {d.id}")
        n            = _ask("n  (ciclos)     ", default_n,            int)
        delay        = _ask("delay  (seg)   ", default_delay,        float)
        read_timeout = _ask("read-timeout   ", default_read_timeout, float)
        timeout      = _ask("timeout (seg)  ", default_timeout,      float)
        workers      = _ask("workers        ", default_workers,       int)
        max_f        = _ask("max-fails      ", default_max_fails,    int,   allow_zero=True)
        backoff      = _ask("backoff (seg)  ", default_backoff,      float)
        # Chars: free-form input, UUIDs separated by spaces
        raw_chars = input(
            f"      {'chars (UUIDs)':<22} [todas] > "
        ).strip()
        chars = raw_chars.split() if raw_chars else (default_chars or [])
        configs.append(
            GattReadConfig(
                address=d.id,
                name=d.nombre,
                n=n,
                delay_s=delay,
                connect_timeout=timeout,
                read_timeout=read_timeout,
                workers=workers,
                char_uuids=chars,
                max_consecutive_fails=max_f,
                backoff_s=backoff,
            )
        )

    print()
    print(sep)
    print(" Resumen de configuración")
    print(sep)
    print(
        f"  {'Dispositivo':<34} {'Nombre':<20} {'n':>4}  "
        f"{'W':>3}  {'Delay':>6}  {'RdTout':>7}  {'Timeout':>7}"
    )
    print("  " + "─" * 72)
    for cfg in configs:
        print(
            f"  {cfg.address:<34} {cfg.name:<20} {cfg.n:>4}  "
            f"{cfg.workers:>3}  {cfg.delay_s:>5.1f}s  "
            f"{cfg.read_timeout:>6.1f}s  {cfg.connect_timeout:>6.1f}s"
        )
    print(sep)
    while True:
        confirm = input("  ¿Iniciar test? [s/N] > ").strip().lower()
        if confirm in ("s", "si", "sí", "y", "yes"):
            return configs
        if confirm in ("", "n", "no"):
            return []


async def main() -> None:
    args = _parse_args()

    # Fase 1 — Escaneo BLE (independiente de la DB)
    entries = await scan_devices()

    # Fase 2 — Persistencia (abre DB, inicializa esquema, guarda y hace commit al salir)
    with get_connection() as conn:
        init_schema(conn)
        procesados, nuevos = persist_entries(conn, entries)

    # Fase 3 — Salida en consola
    print_results(procesados, nuevos)

    # Fase 4 (opcional) — Test de estabilidad interactivo
    if args.test:
        if not procesados:
            print("No se detectaron dispositivos. No es posible ejecutar el test.")
            return

        dispositivos = _pick_devices(procesados)
        if not dispositivos:
            print("Test cancelado.")
            return

        configs = _configure_devices(
            dispositivos,
            default_n=args.n,
            default_delay=args.delay,
            default_timeout=args.timeout,
            default_workers=args.workers,
        )
        if not configs:
            print("Test cancelado.")
            return

        if len(configs) == 1:
            cfg = configs[0]
            await run_stability_test(
                address=cfg.address,
                n=cfg.n,
                delay_s=cfg.delay_s,
                connect_timeout=cfg.connect_timeout,
                workers=cfg.workers,
            )
        else:
            await run_multi_stability_test(configs)

    # Fase 5 (opcional) — Test de conexión repetida
    if args.repeat_test:
        if not procesados:
            print("No se detectaron dispositivos. No es posible ejecutar el test.")
            return

        dispositivos = _pick_devices(procesados)
        if not dispositivos:
            print("Test cancelado.")
            return

        rep_configs = _configure_repeated_devices(
            dispositivos,
            default_n=args.n,
            default_hold=args.hold,
            default_delay=args.delay,
            default_timeout=args.timeout,
            default_workers=args.workers,
            default_max_fails=args.max_fails,
            default_backoff=args.backoff,
        )
        if not rep_configs:
            print("Test cancelado.")
            return

        if len(rep_configs) == 1:
            cfg = rep_configs[0]
            await run_repeated_test(
                address=cfg.address,
                n=cfg.n,
                hold_s=cfg.hold_s,
                delay_s=cfg.delay_s,
                connect_timeout=cfg.connect_timeout,
                workers=cfg.workers,
            )
        else:
            await run_multi_repeated_test(rep_configs)

    # Fase 6 (opcional) — Test de lectura continua GATT
    if args.gatt_test:
        if not procesados:
            print("No se detectaron dispositivos. No es posible ejecutar el test.")
            return

        dispositivos = _pick_devices(procesados)
        if not dispositivos:
            print("Test cancelado.")
            return

        gatt_configs = _configure_gatt_devices(
            dispositivos,
            default_n=args.n,
            default_delay=args.delay,
            default_timeout=args.timeout,
            default_read_timeout=args.read_timeout,
            default_workers=args.workers,
            default_max_fails=args.max_fails,
            default_backoff=args.backoff,
            default_chars=args.chars or None,
        )
        if not gatt_configs:
            print("Test cancelado.")
            return

        if len(gatt_configs) == 1:
            cfg = gatt_configs[0]
            await run_gatt_read_test(
                address=cfg.address,
                n=cfg.n,
                delay_s=cfg.delay_s,
                connect_timeout=cfg.connect_timeout,
                read_timeout=cfg.read_timeout,
                workers=cfg.workers,
                char_uuids=cfg.char_uuids or None,
                max_consecutive_fails=cfg.max_consecutive_fails,
                backoff_s=cfg.backoff_s,
            )
        else:
            await run_multi_gatt_read_test(gatt_configs)


if __name__ == "__main__":
    asyncio.run(main())
