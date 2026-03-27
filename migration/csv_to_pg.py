#!/usr/bin/env python3
"""
Phase 2 — Load exported CSV files into PostgreSQL
==================================================

Reads gzip-compressed CSV files (produced by ``adx_export_to_blob.py``)
from Azure Blob Storage or a local directory and loads them into the
``retail_prices_vm`` table using PostgreSQL ``COPY FROM`` via a temp
staging table.

This is **10–100× faster** than row-by-row INSERT because:
  - ``COPY FROM`` is the fastest PostgreSQL bulk-load path.
  - A temporary staging table avoids per-row conflict checks during COPY.
  - Deduplication happens in a single ``INSERT … ON CONFLICT DO NOTHING``
    statement after each file is loaded into the staging table.

Supports ``--resume`` to skip CSV files that were already successfully loaded
(tracked in the ``job_runs`` table with ``dataset = 'csv_migration'``).

Usage — from Blob Storage:
    python csv_to_pg.py --source blob
    python csv_to_pg.py --source blob --resume
    python csv_to_pg.py --source blob --dry-run

Usage — from local directory (e.g. after azcopy download):
    python csv_to_pg.py --source local --data-dir ./data
    python csv_to_pg.py --source local --data-dir ./data --resume

Environment variables (or .env.local):
    POSTGRES_HOST, POSTGRES_PORT, POSTGRES_DB,
    POSTGRES_USER, POSTGRES_PASSWORD, POSTGRES_SSLMODE

    For --source blob:
    BLOB_ACCOUNT_URL      — e.g. https://<account>.blob.core.windows.net
    BLOB_CONTAINER_NAME   — container holding the exported CSVs
    BLOB_PREFIX           — (optional) filter blobs by prefix (default: pricing_metrics)
    BLOB_SAS_TOKEN        — SAS token for auth (alternative to DefaultAzureCredential)
"""

import argparse
import gzip
import io
import json
import logging
import os
import sys
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path

import psycopg2  # type: ignore[import-untyped]

# ---------------------------------------------------------------------------
# Defaults & constants
# ---------------------------------------------------------------------------
MIGRATION_DATASET = "csv_migration"
DEFAULT_BLOB_PREFIX = "pricing_metrics"

# ADX camelCase header → PG snake_case column
COLUMN_MAP: dict[str, str] = {
    "jobId": "job_id",
    "jobDateTime": "job_datetime",
    "jobType": "job_type",
    "currencyCode": "currency_code",
    "tierMinimumUnits": "tier_minimum_units",
    "retailPrice": "retail_price",
    "unitPrice": "unit_price",
    "armRegionName": "arm_region_name",
    "location": "location",
    "effectiveStartDate": "effective_start_date",
    "meterId": "meter_id",
    "meterName": "meter_name",
    "productId": "product_id",
    "skuId": "sku_id",
    "productName": "product_name",
    "skuName": "sku_name",
    "serviceName": "service_name",
    "serviceId": "service_id",
    "serviceFamily": "service_family",
    "unitOfMeasure": "unit_of_measure",
    "type": "pricing_type",
    "isPrimaryMeterRegion": "is_primary_meter_region",
    "armSkuName": "arm_sku_name",
    "reservationTerm": "reservation_term",
    "savingsPlan": "savings_plan",
}

PG_COLUMNS = list(COLUMN_MAP.values())

# Staging table DDL — uses ADX camelCase headers so COPY FROM matches the CSV
STAGING_DDL = """
CREATE TEMP TABLE staging (
    "jobId"                 TEXT,
    "jobDateTime"           TEXT,
    "jobType"               TEXT,
    "currencyCode"          TEXT,
    "tierMinimumUnits"      TEXT,
    "retailPrice"           TEXT,
    "unitPrice"             TEXT,
    "armRegionName"         TEXT,
    "location"              TEXT,
    "effectiveStartDate"    TEXT,
    "meterId"               TEXT,
    "meterName"             TEXT,
    "productId"             TEXT,
    "skuId"                 TEXT,
    "productName"           TEXT,
    "skuName"               TEXT,
    "serviceName"           TEXT,
    "serviceId"             TEXT,
    "serviceFamily"         TEXT,
    "unitOfMeasure"         TEXT,
    "type"                  TEXT,
    "isPrimaryMeterRegion"  TEXT,
    "armSkuName"            TEXT,
    "reservationTerm"       TEXT,
    "savingsPlan"           TEXT
) ON COMMIT DROP
"""

# INSERT from staging → target with column aliases and type casts
INSERT_FROM_STAGING = """
INSERT INTO retail_prices_vm (
    job_id, job_datetime, job_type,
    currency_code, tier_minimum_units,
    retail_price, unit_price,
    arm_region_name, location,
    effective_start_date,
    meter_id, meter_name,
    product_id, sku_id, product_name, sku_name,
    service_name, service_id, service_family,
    unit_of_measure, pricing_type,
    is_primary_meter_region, arm_sku_name,
    reservation_term, savings_plan
)
SELECT
    "jobId",
    "jobDateTime"::timestamptz,
    "jobType",
    "currencyCode",
    NULLIF("tierMinimumUnits", '')::numeric,
    NULLIF("retailPrice", '')::numeric,
    NULLIF("unitPrice", '')::numeric,
    "armRegionName",
    "location",
    NULLIF("effectiveStartDate", '')::timestamptz,
    "meterId",
    "meterName",
    "productId",
    "skuId",
    "productName",
    "skuName",
    "serviceName",
    "serviceId",
    "serviceFamily",
    "unitOfMeasure",
    "type",
    NULLIF("isPrimaryMeterRegion", '')::boolean,
    "armSkuName",
    "reservationTerm",
    NULLIF("savingsPlan", '')::jsonb
FROM staging
ON CONFLICT (currency_code, arm_region_name, sku_id, pricing_type, reservation_term, job_id)
DO NOTHING
"""

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("csv_to_pg")


# ---------------------------------------------------------------------------
# .env.local loader
# ---------------------------------------------------------------------------
def load_env_file() -> None:
    """Load env vars from .env.local if it exists (key=value lines)."""
    for candidate in [
        Path.cwd() / ".env.local",
        Path(__file__).parent / ".env.local",
    ]:
        if candidate.is_file():
            log.info("Loading env from %s", candidate)
            with open(candidate) as fh:
                for line in fh:
                    line = line.strip()
                    if not line or line.startswith("#"):
                        continue
                    key, _, value = line.partition("=")
                    key, value = key.strip(), value.strip()
                    if key and key not in os.environ:
                        os.environ[key] = value
            return


# ---------------------------------------------------------------------------
# PG helpers
# ---------------------------------------------------------------------------
def create_pg_connection() -> "psycopg2.extensions.connection":
    """Create a psycopg2 connection from env vars."""
    conn = psycopg2.connect(
        host=os.environ.get("POSTGRES_HOST", "localhost"),
        port=os.environ.get("POSTGRES_PORT", "5432"),
        dbname=os.environ.get("POSTGRES_DB", "azscout"),
        user=os.environ.get("POSTGRES_USER", "azscout"),
        password=os.environ.get("POSTGRES_PASSWORD", "azscout"),
        sslmode=os.environ.get("POSTGRES_SSLMODE", "disable"),
    )
    conn.autocommit = False
    return conn


def get_loaded_files(conn: "psycopg2.extensions.connection") -> set[str]:
    """Return CSV filenames already loaded (from job_runs)."""
    with conn.cursor() as cur:
        cur.execute(
            "SELECT details->>'csv_file' FROM job_runs "
            "WHERE dataset = %s AND status = 'ok'",
            (MIGRATION_DATASET,),
        )
        return {row[0] for row in cur.fetchall() if row[0]}


def insert_job_run(
    conn: "psycopg2.extensions.connection",
    run_id: str,
    csv_file: str,
) -> None:
    """Insert a job_runs row for this CSV file."""
    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO job_runs (run_id, dataset, status, started_at_utc, details) "
            "VALUES (%s, %s, %s, %s, %s)",
            (
                run_id,
                MIGRATION_DATASET,
                "running",
                datetime.now(timezone.utc),
                json.dumps({"csv_file": csv_file}),
            ),
        )
    conn.commit()


def update_job_run(
    conn: "psycopg2.extensions.connection",
    run_id: str,
    status: str,
    items_read: int,
    items_written: int,
    error_message: str | None = None,
) -> None:
    """Update a job_runs row after processing."""
    with conn.cursor() as cur:
        cur.execute(
            "UPDATE job_runs SET status = %s, finished_at_utc = %s, "
            "items_read = %s, items_written = %s, error_message = %s "
            "WHERE run_id = %s",
            (
                status,
                datetime.now(timezone.utc),
                items_read,
                items_written,
                error_message,
                run_id,
            ),
        )
    conn.commit()


# ---------------------------------------------------------------------------
# Blob source — list & stream CSV files
# ---------------------------------------------------------------------------
def list_blob_files(prefix: str) -> list[str]:
    """List CSV blob names from Azure Blob Storage."""
    from azure.identity import DefaultAzureCredential as AzCredential
    from azure.storage.blob import ContainerClient

    account_url = os.environ.get("BLOB_ACCOUNT_URL", "")
    container_name = os.environ.get("BLOB_CONTAINER_NAME", "")
    sas_token = os.environ.get("BLOB_SAS_TOKEN")

    if not account_url or not container_name:
        log.error("BLOB_ACCOUNT_URL and BLOB_CONTAINER_NAME must be set")
        sys.exit(1)

    if sas_token:
        container = ContainerClient(
            account_url=account_url,
            container_name=container_name,
            credential=sas_token,
        )
    else:
        container = ContainerClient(
            account_url=account_url,
            container_name=container_name,
            credential=AzCredential(),
        )

    blobs = container.list_blobs(name_starts_with=prefix)
    return sorted(b.name for b in blobs if b.name.endswith((".csv.gz", ".csv")))


def stream_blob_file(blob_name: str) -> io.BytesIO:
    """Download a blob into an in-memory buffer."""
    from azure.identity import DefaultAzureCredential as AzCredential
    from azure.storage.blob import ContainerClient

    account_url = os.environ.get("BLOB_ACCOUNT_URL", "")
    container_name = os.environ.get("BLOB_CONTAINER_NAME", "")
    sas_token = os.environ.get("BLOB_SAS_TOKEN")

    if sas_token:
        container = ContainerClient(
            account_url=account_url,
            container_name=container_name,
            credential=sas_token,
        )
    else:
        container = ContainerClient(
            account_url=account_url,
            container_name=container_name,
            credential=AzCredential(),
        )

    blob_client = container.get_blob_client(blob_name)
    buf = io.BytesIO()
    blob_client.download_blob().readinto(buf)
    buf.seek(0)
    return buf


# ---------------------------------------------------------------------------
# Local source — list & stream CSV files
# ---------------------------------------------------------------------------
def list_local_files(data_dir: str, prefix: str) -> list[str]:
    """List CSV files from a local directory."""
    p = Path(data_dir)
    if not p.is_dir():
        log.error("Data directory does not exist: %s", data_dir)
        sys.exit(1)
    files = sorted(
        f.name
        for f in p.iterdir()
        if f.is_file()
        and f.name.startswith(prefix)
        and f.name.endswith((".csv.gz", ".csv"))
    )
    return files


def stream_local_file(data_dir: str, filename: str) -> io.BytesIO:
    """Read a local file into an in-memory buffer."""
    filepath = Path(data_dir) / filename
    buf = io.BytesIO(filepath.read_bytes())
    return buf


# ---------------------------------------------------------------------------
# CSV → PG loader (core logic)
# ---------------------------------------------------------------------------
def load_csv_to_pg(
    conn: "psycopg2.extensions.connection",
    csv_data: io.BytesIO,
    is_gzipped: bool,
) -> tuple[int, int]:
    """
    Load a single CSV file into retail_prices_vm via a temp staging table.

    Returns (rows_read, rows_written).
    """
    # Decompress if gzipped
    if is_gzipped:
        text_stream = io.TextIOWrapper(gzip.open(csv_data, "rb"), encoding="utf-8")
    else:
        text_stream = io.TextIOWrapper(csv_data, encoding="utf-8")

    with conn.cursor() as cur:
        # Create temp staging table (dropped on commit)
        cur.execute(STAGING_DDL)

        # COPY CSV into staging table
        copy_sql = "COPY staging FROM STDIN WITH (FORMAT csv, HEADER true, NULL '')"
        cur.copy_expert(copy_sql, text_stream)  # type: ignore[arg-type]
        rows_read = cur.rowcount

        # Move from staging to target with dedup
        cur.execute(INSERT_FROM_STAGING)
        rows_written = cur.rowcount

    conn.commit()
    text_stream.close()
    return rows_read, rows_written


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def run(args: argparse.Namespace) -> None:
    load_env_file()

    log.info("Source       : %s", args.source)
    log.info("Resume       : %s", args.resume)
    log.info("Dry-run      : %s", args.dry_run)

    prefix = os.environ.get("BLOB_PREFIX", DEFAULT_BLOB_PREFIX)

    # --- List files ---
    if args.source == "blob":
        log.info("Listing CSV files from Blob Storage (prefix=%s) …", prefix)
        all_files = list_blob_files(prefix)
    else:
        log.info("Listing CSV files from %s (prefix=%s) …", args.data_dir, prefix)
        all_files = list_local_files(args.data_dir, prefix)

    log.info("Found %d CSV file(s)", len(all_files))

    if not all_files:
        log.warning("No CSV files found — nothing to do")
        return

    # --- PG connection ---
    log.info("Connecting to PostgreSQL …")
    pg_conn = create_pg_connection()

    try:
        # --- Resume support ---
        skip_files: set[str] = set()
        if args.resume:
            skip_files = get_loaded_files(pg_conn)
            log.info("Resume: %d file(s) already loaded, will skip", len(skip_files))

        pending = [f for f in all_files if f not in skip_files]
        log.info("Files to load: %d", len(pending))

        if args.dry_run:
            log.info("DRY-RUN — no data will be written")
            for f in pending:
                log.info("  would load: %s", f)
            return

        # --- Process each file ---
        grand_read = 0
        grand_written = 0
        start = time.monotonic()

        for idx, filename in enumerate(pending, 1):
            run_id = str(uuid.uuid4())
            is_gzipped = filename.endswith(".gz")

            log.info("[%d/%d] Loading %s …", idx, len(pending), filename)
            insert_job_run(pg_conn, run_id, filename)

            try:
                # Get file data
                if args.source == "blob":
                    csv_data = stream_blob_file(filename)
                else:
                    csv_data = stream_local_file(args.data_dir, filename)

                rows_read, rows_written = load_csv_to_pg(
                    pg_conn,
                    csv_data,
                    is_gzipped,
                )
                csv_data.close()

                grand_read += rows_read
                grand_written += rows_written

                log.info(
                    "  %d rows read, %d written (%d skipped/dedup)",
                    rows_read,
                    rows_written,
                    rows_read - rows_written,
                )
                update_job_run(pg_conn, run_id, "ok", rows_read, rows_written)

            except Exception as exc:
                log.error("  FAILED %s: %s", filename, exc)
                try:
                    pg_conn.rollback()
                except Exception:
                    pass
                update_job_run(pg_conn, run_id, "error", 0, 0, str(exc)[:500])
                continue

        elapsed = time.monotonic() - start
        log.info("=" * 60)
        log.info("Import complete")
        log.info("  Files processed : %d / %d", len(pending), len(all_files))
        log.info("  Total read      : %d", grand_read)
        log.info("  Total written   : %d", grand_written)
        log.info("  Total dedup     : %d", grand_read - grand_written)
        log.info("  Elapsed         : %.1f s (%.1f min)", elapsed, elapsed / 60)

    finally:
        pg_conn.close()
        log.info("PostgreSQL connection closed")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------
def main() -> None:
    parser = argparse.ArgumentParser(
        description="Load ADX-exported CSV files into PostgreSQL retail_prices_vm",
    )
    parser.add_argument(
        "--source",
        choices=["blob", "local"],
        default="blob",
        help="Where to read CSV files from (default: blob)",
    )
    parser.add_argument(
        "--data-dir",
        default="./data",
        help="Local directory with CSV files (only for --source local)",
    )
    parser.add_argument(
        "--resume",
        action="store_true",
        help="Skip CSV files already loaded (matched via job_runs)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="List files without loading any data",
    )
    args = parser.parse_args()

    if args.source == "local" and not Path(args.data_dir).is_dir():
        parser.error(f"--data-dir '{args.data_dir}' is not a directory")

    run(args)


if __name__ == "__main__":
    main()
