from typing import Any

from bleak import BleakScanner

from utils import to_json_safe

SCAN_TIMEOUT_SECONDS = 8.0

# Bluetooth SIG Company Identifiers — https://www.bluetooth.com/specifications/assigned-numbers/
_MANUFACTURER_NAMES: dict[int, str] = {
    0x0002: "Intel",
    0x0006: "Microsoft",
    0x000F: "Broadcom",
    0x004C: "Apple",
    0x0057: "Harman International",
    0x0059: "Nordic Semiconductor",
    0x0075: "Samsung Electronics",
    0x00D2: "Fitbit",
    0x00E0: "Google",
    0x0131: "Xiaomi",
    0x0171: "Amazon",
    0x01D9: "Bose",
    0x0499: "Ruuvi Innovations",
    0x04F8: "Huawei",
    0x058E: "Sony",
    0x0660: "LEGO",
    0x06D3: "Jabra (GN Audio)",
}


def extract_manufacturer_name(manufacturer_data: dict | None) -> str | None:
    """Returns the company name for the first manufacturer ID in the payload, or None."""
    if not manufacturer_data:
        return None
    company_id = next(iter(manufacturer_data))
    return _MANUFACTURER_NAMES.get(company_id, f"ID 0x{company_id:04X}")


def normalize_scan_results(scan_results: Any) -> list[tuple[Any, Any]]:
    """
    Normalizes BleakScanner.discover() output to a uniform list.

    When return_adv=True discover() returns dict[address, (BLEDevice, AdvertisementData)].
    Fallback handles a plain list of BLEDevice (older Bleak or alternate call).

    Returns:
        List of (BLEDevice, AdvertisementData | None) tuples.
    """
    if isinstance(scan_results, dict):
        return list(scan_results.values())
    return [(device, None) for device in scan_results]


def build_metadata(device: Any, adv: Any) -> dict[str, Any]:
    """
    Builds a complete metadata dict from a BLEDevice and AdvertisementData pair.
    All values are converted to JSON-safe types via to_json_safe().
    """
    return {
        "device_address": getattr(device, "address", None),
        "device_name": getattr(device, "name", None),
        "device_details": to_json_safe(getattr(device, "details", None)),
        "adv": {
            "local_name": getattr(adv, "local_name", None),
            "rssi": getattr(adv, "rssi", None),
            "tx_power": getattr(adv, "tx_power", None),
            "service_uuids": to_json_safe(getattr(adv, "service_uuids", None)),
            "manufacturer_data": to_json_safe(getattr(adv, "manufacturer_data", None)),
            "service_data": to_json_safe(getattr(adv, "service_data", None)),
            "platform_data": to_json_safe(getattr(adv, "platform_data", None)),
        },
    }


async def scan_devices() -> list[tuple[Any, Any]]:
    """
    Runs a BLE scan and returns normalized (BLEDevice, AdvertisementData | None) tuples.
    Uses return_adv=True to capture full advertisement payloads.
    """
    results = await BleakScanner.discover(
        timeout=SCAN_TIMEOUT_SECONDS,
        return_adv=True,
    )
    return normalize_scan_results(results)
