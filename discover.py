"""
discover.py — Descubrimiento completo de Bluetooth desde Python.

Detecta:
  1. Adaptadores BT del sistema (hci0, hci1, ...) con versión, features y TX power
  2. Dispositivos BLE cercanos (bleak BleakScanner)
  3. Dispositivos BR/EDR Classic cercanos (hcitool scan via subprocess)

Uso:
    python discover.py                  # todo
    python discover.py --ble-only
    python discover.py --classic-only
    python discover.py --adapters-only
    python discover.py --timeout 15     # escaneo BLE más largo
"""

import argparse
import asyncio
import re
import subprocess
import sys
from dataclasses import dataclass

from bleak import BleakScanner
from bleak.backends.device import BLEDevice
from bleak.backends.scanner import AdvertisementData


# ---------------------------------------------------------------------------
# 1. Adaptadores
# ---------------------------------------------------------------------------

@dataclass
class AdapterInfo:
    name: str          # hci0, hci1, ...
    bd_addr: str
    hci_version: str
    lmp_version: str
    manufacturer: str
    features: list[str]
    is_up: bool
    raw: str


_HCI_VERSION_MAP = {
    "0x00": "BT 1.0b", "0x01": "BT 1.1", "0x02": "BT 1.2",
    "0x03": "BT 2.0",  "0x04": "BT 2.1", "0x05": "BT 3.0",
    "0x06": "BT 4.0",  "0x07": "BT 4.1", "0x08": "BT 4.2",
    "0x09": "BT 4.2",  "0x0a": "BT 5.0", "0x0b": "BT 5.1",
    "0x0c": "BT 5.2",  "0x0d": "BT 5.3", "0x0e": "BT 5.4",
}


def _parse_adapter(block: str) -> AdapterInfo | None:
    """Parse one hciconfig -a block into AdapterInfo."""
    name_m = re.search(r"^(hci\d+)", block, re.MULTILINE)
    if not name_m:
        return None

    name = name_m.group(1)

    addr_m = re.search(r"BD Address:\s*([\w:]+)", block)
    bd_addr = addr_m.group(1) if addr_m else "?"

    is_up = "UP RUNNING" in block or "UP" in block

    hci_m = re.search(r"HCI\s+Version:\s*(0x[\da-fA-F]+|\d+)", block)
    raw_ver = hci_m.group(1).lower() if hci_m else "?"
    # Normalize to hex string for lookup
    if raw_ver.isdigit():
        raw_ver = hex(int(raw_ver))
    hci_version = _HCI_VERSION_MAP.get(raw_ver, f"HCI {raw_ver}")

    lmp_m = re.search(r"LMP\s+Version:\s*(0x[\da-fA-F]+|\d+)", block)
    raw_lmp = lmp_m.group(1).lower() if lmp_m else "?"
    if raw_lmp.isdigit():
        raw_lmp = hex(int(raw_lmp))
    lmp_version = _HCI_VERSION_MAP.get(raw_lmp, f"LMP {raw_lmp}")

    mfr_m = re.search(r"Manufacturer\s*[:\(]([^\n\)]+)", block)
    manufacturer = mfr_m.group(1).strip() if mfr_m else "?"

    features: list[str] = []
    feat_m = re.search(r"Features[^\n]*\n(.*?)(?:\n\s*\n|\Z)", block, re.DOTALL)
    if feat_m:
        features = [f.strip() for f in re.split(r"[\t ]{2,}", feat_m.group(1)) if f.strip()]

    return AdapterInfo(
        name=name,
        bd_addr=bd_addr,
        hci_version=hci_version,
        lmp_version=lmp_version,
        manufacturer=manufacturer,
        features=features,
        is_up=is_up,
        raw=block,
    )


def discover_adapters() -> list[AdapterInfo]:
    """Returns all BT adapters found by hciconfig -a."""
    try:
        out = subprocess.check_output(["hciconfig", "-a"], text=True, stderr=subprocess.DEVNULL)
    except (FileNotFoundError, subprocess.CalledProcessError):
        return []

    # Split on blank line between adapter blocks
    blocks = re.split(r"\n(?=hci\d+)", out)
    adapters = []
    for block in blocks:
        info = _parse_adapter(block)
        if info:
            adapters.append(info)
    return adapters


def print_adapters(adapters: list[AdapterInfo]) -> None:
    sep = "═" * 72
    sep2 = "─" * 72
    print(sep)
    print(f" Adaptadores Bluetooth  ({len(adapters)} encontrados)")
    print(sep)
    for a in adapters:
        estado = "▲ ACTIVO" if a.is_up else "▼ inactivo"
        print(f"  {a.name}  {a.bd_addr}  [{estado}]")
        print(f"    Versión    : {a.hci_version}  (LMP: {a.lmp_version})")
        print(f"    Fabricante : {a.manufacturer}")
        if a.features:
            print(f"    Features   : {', '.join(a.features[:6])}")
        print(sep2)


# ---------------------------------------------------------------------------
# 2. BLE scan (bleak)
# ---------------------------------------------------------------------------

@dataclass
class BleDevice:
    address: str
    name: str
    rssi: int | None
    manufacturer: str | None
    services: list[str]
    tx_power: int | None


_MANUFACTURER_NAMES: dict[int, str] = {
    0x0002: "Intel",       0x000F: "Broadcom",    0x004C: "Apple",
    0x0057: "Harman",      0x0059: "Nordic",      0x0075: "Samsung",
    0x00E0: "Google",      0x0131: "Xiaomi",      0x0171: "Amazon",
    0x01D9: "Bose",        0x04F8: "Huawei",      0x058E: "Sony",
    0x06D3: "Jabra",
}


def _extract_mfr(adv: AdvertisementData | None) -> str | None:
    if not adv or not adv.manufacturer_data:
        return None
    cid = next(iter(adv.manufacturer_data))
    return _MANUFACTURER_NAMES.get(cid, f"ID 0x{cid:04X}")


async def discover_ble(timeout: float = 10.0) -> list[BleDevice]:
    """Scans for BLE devices and returns a sorted list."""
    print(f"\n  Escaneando BLE ({timeout}s)...", end="", flush=True)
    results: dict[str, tuple[BLEDevice, AdvertisementData]] = await BleakScanner.discover(
        timeout=timeout, return_adv=True
    )
    print(f" {len(results)} encontrados")

    devices = []
    for dev, adv in results.values():
        devices.append(BleDevice(
            address=dev.address,
            name=dev.name or "—",
            rssi=adv.rssi if adv else None,
            manufacturer=_extract_mfr(adv),
            services=[str(u) for u in (adv.service_uuids if adv else [])],
            tx_power=adv.tx_power if adv else None,
        ))
    return sorted(devices, key=lambda d: d.rssi or -999, reverse=True)


def print_ble_devices(devices: list[BleDevice]) -> None:
    sep = "═" * 72
    sep2 = "─" * 72
    print(sep)
    print(f" Dispositivos BLE  ({len(devices)} encontrados)  — ordenados por RSSI")
    print(sep)
    print(f"  {'#':>3}  {'Dirección':<38}  {'RSSI':>5}  {'Nombre':<24}  Fabricante")
    print(sep2)
    for i, d in enumerate(devices, 1):
        rssi = f"{d.rssi} dBm" if d.rssi is not None else "  —"
        mfr  = d.manufacturer or "—"
        print(f"  {i:>3}  {d.address:<38}  {rssi:>7}  {d.name:<24}  {mfr}")
        if d.services:
            for svc in d.services[:3]:
                print(f"       └─ {svc}")
            if len(d.services) > 3:
                print(f"       └─ ... ({len(d.services) - 3} más)")
    print(sep)


# ---------------------------------------------------------------------------
# 3. BR/EDR Classic scan (hcitool + sdptool)
# ---------------------------------------------------------------------------

@dataclass
class ClassicDevice:
    address: str
    name: str
    services: list[str]


def discover_classic(timeout: int = 15) -> list[ClassicDevice]:
    """Runs hcitool scan and optionally sdptool browse for each device."""
    print(f"\n  Escaneando BR/EDR Classic ({timeout}s)...", end="", flush=True)
    try:
        out = subprocess.check_output(
            ["hcitool", "scan", "--flush"],
            text=True,
            timeout=timeout + 5,
            stderr=subprocess.DEVNULL,
        )
    except (FileNotFoundError, subprocess.TimeoutExpired, subprocess.CalledProcessError):
        print(" hcitool no disponible o timeout")
        return []

    devices = []
    for line in out.splitlines():
        m = re.match(r"\s+([\dA-Fa-f:]{17})\s+(.*)", line)
        if m:
            addr, name = m.group(1), m.group(2).strip() or "—"
            devices.append(ClassicDevice(address=addr, name=name, services=[]))

    print(f" {len(devices)} encontrados")

    # Fetch services via sdptool (best-effort, skip on timeout)
    for dev in devices:
        try:
            sdp_out = subprocess.check_output(
                ["sdptool", "browse", "--nobreaker", dev.address],
                text=True, timeout=8, stderr=subprocess.DEVNULL,
            )
            services = re.findall(r"Service Name:\s*(.+)", sdp_out)
            dev.services = services[:10]
        except Exception:
            pass

    return devices


def print_classic_devices(devices: list[ClassicDevice]) -> None:
    sep = "═" * 72
    sep2 = "─" * 72
    print(sep)
    print(f" Dispositivos BR/EDR Classic  ({len(devices)} encontrados)")
    print(sep)
    if not devices:
        print("  (ninguno en rango o dispositivos no visibles)")
        print(sep)
        return
    for i, d in enumerate(devices, 1):
        print(f"  {i:>3}  {d.address}   {d.name}")
        for svc in d.services:
            print(f"       └─ {svc}")
    print(sep)


# ---------------------------------------------------------------------------
# Orchestrator
# ---------------------------------------------------------------------------

async def main(args: argparse.Namespace) -> None:
    do_adapters = args.adapters_only or not (args.ble_only or args.classic_only)
    do_ble      = args.ble_only      or not (args.adapters_only or args.classic_only)
    do_classic  = args.classic_only  or not (args.adapters_only or args.ble_only)

    print()
    print("═" * 72)
    print("  Bluetooth Discovery — Python")
    print("═" * 72)

    if do_adapters:
        adapters = discover_adapters()
        if adapters:
            print_adapters(adapters)
            # Suggest the best adapter
            best = max(
                adapters,
                key=lambda a: int(
                    re.search(r"0x([\da-f]+)", a.hci_version.split()[-1].lower()
                              if a.hci_version != "?" else "0x00").group(1), 16
                ) if re.search(r"0x[\da-f]+", a.hci_version, re.IGNORECASE) else 0,
                default=None,
            )
            if best:
                print(f"  ★ Adaptador más potente : {best.name}  {best.hci_version}  {best.bd_addr}\n")
        else:
            print("  ⚠  No se encontraron adaptadores (¿hciconfig instalado?)")

    if do_ble:
        ble_devs = await discover_ble(timeout=args.timeout)
        print_ble_devices(ble_devs)

    if do_classic:
        classic_devs = discover_classic(timeout=int(args.timeout))
        print_classic_devices(classic_devs)


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Descubrimiento completo de Bluetooth (adaptadores + BLE + BR/EDR)",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument("--timeout",       type=float, default=10.0,
                        help="Duración del escaneo en segundos")
    parser.add_argument("--ble-only",      action="store_true",
                        help="Solo escanear BLE")
    parser.add_argument("--classic-only",  action="store_true",
                        help="Solo escanear BR/EDR Classic")
    parser.add_argument("--adapters-only", action="store_true",
                        help="Solo listar adaptadores")
    return parser.parse_args()


if __name__ == "__main__":
    # Root check (needed for BR/EDR raw sockets on Linux)
    import os
    if sys.platform == "linux" and os.geteuid() != 0:
        print("⚠  Recomendado ejecutar con sudo para escaneo BR/EDR completo.")

    asyncio.run(main(_parse_args()))
