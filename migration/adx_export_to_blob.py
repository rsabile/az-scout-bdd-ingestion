#!/usr/bin/env python3
"""
Phase 1 — ADX server-side export to Azure Blob Storage (CSV)
=============================================================

Runs the ADX ``.export to csv`` management command so that the ADX cluster
writes compressed CSV files directly to a Blob container.  No data passes
through the local machine.

Usage:
    python adx_export_to_blob.py
    python adx_export_to_blob.py --dry-run
    python adx_export_to_blob.py --prefix my_export_2025
    python adx_export_to_blob.py --size-limit 512  # MB per file

Environment variables (or .env.local):
    ADX_CLUSTER_URI       — ADX cluster URL
    ADX_DATABASE_NAME     — ADX database name
    ADX_TENANT_ID         — (optional) tenant for guest accounts
    BLOB_STORAGE_CONN_STR — Blob connection string
                            (format: https://<account>.blob.core.windows.net/<container>;SecretKey=<key>)

See .env.example for a template.
"""

import argparse
import logging
import os
import sys
import time
from pathlib import Path

from azure.identity import DefaultAzureCredential
from azure.kusto.data import KustoClient, KustoConnectionStringBuilder
from azure.kusto.data.exceptions import KustoThrottlingError

# ---------------------------------------------------------------------------
# Defaults
# ---------------------------------------------------------------------------
DEFAULT_ADX_CLUSTER = "https://az-pricing-tool-adx.germanywestcentral.kusto.windows.net"
DEFAULT_ADX_DATABASE = "pricing-metrics"
DEFAULT_PREFIX = "pricing_metrics"
DEFAULT_SIZE_LIMIT_MB = 1024  # 1 GB per CSV chunk
MAX_RETRIES = 5
INITIAL_BACKOFF_SECS = 30

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("adx_export_to_blob")


# ---------------------------------------------------------------------------
# .env.local loader  (same as migrate_adx_to_pg.py)
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
# ADX client  (same auth pattern as migrate_adx_to_pg.py)
# ---------------------------------------------------------------------------
def create_adx_client(cluster_uri: str, tenant_id: str | None) -> KustoClient:
    """Authenticate to ADX using DefaultAzureCredential with auto-refresh."""
    kwargs: dict[str, object] = {"exclude_managed_identity_credential": True}
    if tenant_id:
        kwargs["additionally_allowed_tenants"] = [tenant_id]

    credential = DefaultAzureCredential(**kwargs)
    scope = "https://help.kusto.windows.net/.default"

    def token_provider() -> str:
        return credential.get_token(scope).token

    kcsb = KustoConnectionStringBuilder.with_token_provider(
        cluster_uri,
        token_provider,
    )
    return KustoClient(kcsb)


# ---------------------------------------------------------------------------
# .export command builder
# ---------------------------------------------------------------------------
def build_export_command(
    blob_conn_str: str,
    prefix: str,
    size_limit_mb: int,
) -> str:
    """Build the KQL `.export to csv` management command."""
    size_limit_bytes = size_limit_mb * 1024 * 1024
    return (
        f".export to csv (\n"
        f'    h@"{blob_conn_str}"\n'
        f")\n"
        f"with (\n"
        f'    namePrefix = "{prefix}",\n'
        f'    includeHeaders = "all",\n'
        f"    compressed = true,\n"
        f"    sizeLimit = {size_limit_bytes}\n"
        f")\n"
        f"<| pricing_metrics"
    )


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def export(args: argparse.Namespace) -> None:
    load_env_file()

    cluster_uri = os.environ.get("ADX_CLUSTER_URI", DEFAULT_ADX_CLUSTER)
    database = os.environ.get("ADX_DATABASE_NAME", DEFAULT_ADX_DATABASE)
    tenant_id = os.environ.get("ADX_TENANT_ID")
    blob_conn_str = os.environ.get("BLOB_STORAGE_CONN_STR", "")

    if not blob_conn_str:
        log.error(
            "BLOB_STORAGE_CONN_STR is not set.  "
            "Expected format: https://<account>.blob.core.windows.net/<container>;SecretKey=<key>"
        )
        sys.exit(1)

    log.info("ADX cluster    : %s", cluster_uri)
    log.info("ADX database   : %s", database)
    log.info("Blob target    : %s…", blob_conn_str[:60])
    log.info("Name prefix    : %s", args.prefix)
    log.info("Size limit     : %d MB per file", args.size_limit)

    command = build_export_command(blob_conn_str, args.prefix, args.size_limit)

    if args.dry_run:
        log.info("DRY-RUN — command that would be executed:\n%s", command)
        return

    log.info("Connecting to ADX …")
    adx = create_adx_client(cluster_uri, tenant_id)

    log.info("Starting server-side export (this may take several minutes) …")
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            response = adx.execute_mgmt(database, command)
            break
        except KustoThrottlingError:
            if attempt == MAX_RETRIES:
                log.error(
                    "ADX still throttling after %d attempts — giving up.", MAX_RETRIES
                )
                sys.exit(1)
            wait = INITIAL_BACKOFF_SECS * (2 ** (attempt - 1))
            log.warning(
                "ADX returned 429 (throttled). Retry %d/%d in %d s …",
                attempt,
                MAX_RETRIES,
                wait,
            )
            time.sleep(wait)

    primary = response.primary_results[0]

    log.info("Export completed.  Exported files:")
    total_records = 0
    file_count = 0
    for row in primary:
        path = row["Path"]
        num_records = row["NumRecords"]
        size_bytes = row["SizeInBytes"]
        total_records += num_records
        file_count += 1
        size_mb = size_bytes / (1024 * 1024)
        log.info("  %s  (%d records, %.1f MB)", path, num_records, size_mb)

    log.info("=" * 60)
    log.info("Total files   : %d", file_count)
    log.info("Total records : %d", total_records)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Export ADX pricing_metrics to Azure Blob Storage as CSV",
    )
    parser.add_argument(
        "--prefix",
        default=DEFAULT_PREFIX,
        help=f"Blob name prefix (default: {DEFAULT_PREFIX})",
    )
    parser.add_argument(
        "--size-limit",
        type=int,
        default=DEFAULT_SIZE_LIMIT_MB,
        help=f"Max size per CSV file in MB (default: {DEFAULT_SIZE_LIMIT_MB})",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print the export command without executing",
    )
    args = parser.parse_args()
    export(args)


if __name__ == "__main__":
    main()
