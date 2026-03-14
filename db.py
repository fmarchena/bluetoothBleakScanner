import sqlite3
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Generator

DB_PATH = Path(__file__).with_name("ble_tracking.db")


@dataclass(frozen=True, slots=True)
class DeviceRecord:
    """Snapshot of a device's tracking state after an upsert."""
    is_new: bool
    total: int
    first_seen: str
    last_seen: str
    rssi_avg: float | None


@contextmanager
def get_connection() -> Generator[sqlite3.Connection, None, None]:
    """Opens a DB connection, commits on success, rolls back on any exception."""
    conn = sqlite3.connect(DB_PATH)
    conn.execute("PRAGMA foreign_keys = ON")
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def init_schema(conn: sqlite3.Connection) -> None:
    """Creates tables if they do not exist and migrates existing DBs."""
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS devices (
            id                     TEXT    PRIMARY KEY,
            name                   TEXT,
            first_seen             TEXT    NOT NULL,
            last_seen              TEXT    NOT NULL,
            total_appearances      INTEGER NOT NULL DEFAULT 0,
            last_rssi              INTEGER,
            rssi_sum               INTEGER NOT NULL DEFAULT 0,
            rssi_count             INTEGER NOT NULL DEFAULT 0,
            rssi_avg               REAL,
            manufacturer_name      TEXT,
            service_uuids_json     TEXT,
            manufacturer_data_json TEXT,
            service_data_json      TEXT,
            tx_power               INTEGER,
            platform_data_json     TEXT
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS sightings (
            sighting_id   INTEGER PRIMARY KEY AUTOINCREMENT,
            device_id     TEXT    NOT NULL,
            seen_at       TEXT    NOT NULL,
            name          TEXT,
            rssi          INTEGER,
            metadata_json TEXT,
            FOREIGN KEY(device_id) REFERENCES devices(id)
        )
        """
    )
    _migrate_schema(conn)


def _migrate_schema(conn: sqlite3.Connection) -> None:
    """Adds columns introduced in newer versions to existing databases."""
    for sql in (
        "ALTER TABLE devices ADD COLUMN rssi_sum INTEGER NOT NULL DEFAULT 0",
        "ALTER TABLE devices ADD COLUMN rssi_count INTEGER NOT NULL DEFAULT 0",
        "ALTER TABLE devices ADD COLUMN rssi_avg REAL",
        "ALTER TABLE devices ADD COLUMN manufacturer_name TEXT",
    ):
        try:
            conn.execute(sql)
        except sqlite3.OperationalError:
            pass  # column already exists


def upsert_device(
    conn: sqlite3.Connection,
    *,
    device_id: str,
    name: str,
    seen_at: str,
    rssi: int | None,
    manufacturer_name: str | None,
    service_uuids_json: str,
    manufacturer_data_json: str,
    service_data_json: str,
    tx_power: int | None,
    platform_data_json: str,
) -> DeviceRecord:
    """
    Inserts a new device or updates an existing one.
    Maintains a running RSSI average (rssi_sum / rssi_count) across all sightings.

    Returns:
        DeviceRecord with is_new, total, first_seen, last_seen, rssi_avg.
    """
    row = conn.execute(
        "SELECT total_appearances, first_seen, rssi_sum, rssi_count FROM devices WHERE id = ?",
        (device_id,),
    ).fetchone()

    rssi_delta = rssi if rssi is not None else 0
    rssi_count_delta = 1 if rssi is not None else 0

    if row is None:
        new_sum = rssi_delta
        new_count = rssi_count_delta
        new_avg = float(rssi) if rssi is not None else None
        conn.execute(
            """
            INSERT INTO devices (
                id, name, first_seen, last_seen, total_appearances, last_rssi,
                rssi_sum, rssi_count, rssi_avg, manufacturer_name,
                service_uuids_json, manufacturer_data_json, service_data_json,
                tx_power, platform_data_json
            ) VALUES (?, ?, ?, ?, 1, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                device_id, name, seen_at, seen_at, rssi,
                new_sum, new_count, new_avg, manufacturer_name,
                service_uuids_json, manufacturer_data_json, service_data_json,
                tx_power, platform_data_json,
            ),
        )
        return DeviceRecord(
            is_new=True, total=1,
            first_seen=seen_at, last_seen=seen_at,
            rssi_avg=new_avg,
        )

    total, first_seen, old_sum, old_count = (
        int(row[0]), row[1], int(row[2] or 0), int(row[3] or 0)
    )
    total += 1
    new_sum = old_sum + rssi_delta
    new_count = old_count + rssi_count_delta
    new_avg = round(new_sum / new_count, 1) if new_count > 0 else None
    conn.execute(
        """
        UPDATE devices
        SET
            name                   = ?,
            last_seen              = ?,
            total_appearances      = ?,
            last_rssi              = ?,
            rssi_sum               = ?,
            rssi_count             = ?,
            rssi_avg               = ?,
            manufacturer_name      = ?,
            service_uuids_json     = ?,
            manufacturer_data_json = ?,
            service_data_json      = ?,
            tx_power               = ?,
            platform_data_json     = ?
        WHERE id = ?
        """,
        (
            name, seen_at, total, rssi,
            new_sum, new_count, new_avg, manufacturer_name,
            service_uuids_json, manufacturer_data_json, service_data_json,
            tx_power, platform_data_json,
            device_id,
        ),
    )
    return DeviceRecord(
        is_new=False, total=total,
        first_seen=first_seen, last_seen=seen_at,
        rssi_avg=new_avg,
    )


def insert_sighting(
    conn: sqlite3.Connection,
    *,
    device_id: str,
    seen_at: str,
    name: str,
    rssi: int | None,
    metadata_json: str,
) -> None:
    """Inserts one sighting record for a single device scan event."""
    conn.execute(
        """
        INSERT INTO sightings (device_id, seen_at, name, rssi, metadata_json)
        VALUES (?, ?, ?, ?, ?)
        """,
        (device_id, seen_at, name, rssi, metadata_json),
    )
