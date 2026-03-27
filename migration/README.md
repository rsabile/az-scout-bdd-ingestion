# ADX → PostgreSQL Migration

Migrates all historical pricing data from Azure Data Explorer (`pricing_metrics`, ~186M rows, 225 daily snapshots) into the PostgreSQL `retail_prices_vm` table.

## Two approaches

| Approach | Script(s) | Speed | Best for |
|---|---|---|---|
| **CSV pipeline (recommended)** | `adx_export_to_blob.py` → `csv_to_pg.py` | ~100-500K rows/sec | Large datasets (>10M rows) |
| **Direct (legacy)** | `migrate_adx_to_pg.py` | ~5-10K rows/sec | Small datasets, debugging |

The CSV pipeline is **10-100× faster** because:
- Phase 1 runs entirely server-side on the ADX cluster (no data through the client).
- Phase 2 uses PostgreSQL `COPY FROM` instead of row-by-row INSERT.

---

## Prerequisites

1. **Azure CLI login** with access to the ADX cluster:
   ```bash
   az login --tenant <TENANT_ID>
   ```
2. **PostgreSQL** `retail_prices_vm` table with the updated UNIQUE constraint that includes `job_id` (see `alter_unique.sql`).
3. **Python 3.11+**
4. **Azure Blob Storage** container for the CSV export (CSV pipeline only).

## Setup

```bash
cd migration/
uv venv && uv pip install -r requirements.txt

# Copy and fill in credentials
cp .env.example .env.local
# Edit .env.local
```

---

## CSV pipeline (recommended)

### Phase 1 — Export ADX → Blob Storage

The ADX cluster writes compressed CSV files directly to a Blob container.
No data passes through your local machine.

```bash
# Preview the export command without executing
uv run adx_export_to_blob.py --dry-run

# Run the export
uv run adx_export_to_blob.py

# Custom file prefix and size limit
uv run adx_export_to_blob.py --prefix my_export --size-limit 512
```

**Required env vars:** `ADX_CLUSTER_URI`, `ADX_DATABASE_NAME`, `BLOB_STORAGE_CONN_STR`

The script logs the exported file list (names, row counts, sizes).

### Phase 2 — Load CSVs → PostgreSQL

Reads the exported CSV files and loads them into `retail_prices_vm` using `COPY FROM` + a temp staging table.

#### From Blob Storage (streaming)

```bash
# Preview — list files without loading
uv run csv_to_pg.py --source blob --dry-run

# Full import
uv run csv_to_pg.py --source blob

# Resume after interruption
uv run csv_to_pg.py --source blob --resume
```

**Required env vars:** `BLOB_ACCOUNT_URL`, `BLOB_CONTAINER_NAME`, plus Postgres vars.

#### From local directory (after azcopy)

If you prefer to download CSVs first (e.g. with `azcopy`), use local mode:

```bash
azcopy copy "https://<account>.blob.core.windows.net/<container>/pricing_metrics*" ./data/

uv run csv_to_pg.py --source local --data-dir ./data
uv run csv_to_pg.py --source local --data-dir ./data --resume
```

### How the CSV pipeline works

1. **Phase 1:** `adx_export_to_blob.py` runs a `.export to csv` management command on ADX. The ADX cluster writes gzip-compressed CSV files (with headers) to Blob Storage. Each file is ~1 GB (configurable via `--size-limit`).
2. **Phase 2:** `csv_to_pg.py` iterates over the CSV files. For each file:
   - Creates a temporary staging table with camelCase columns matching the CSV headers.
   - Loads the CSV via `COPY staging FROM STDIN` (fastest PG bulk load).
   - Runs `INSERT INTO retail_prices_vm SELECT ... FROM staging ON CONFLICT DO NOTHING` to move data to the target with deduplication and column renaming.
   - Records the file in `job_runs` (dataset `csv_migration`).
3. **Resume:** `--resume` queries `job_runs WHERE dataset='csv_migration' AND status='ok'` to skip already-loaded files.

---

## Direct migration (legacy)

For small datasets or debugging. Queries ADX per-job and batch-INSERTs into PG.

```bash
# Dry run
uv run migrate_adx_to_pg.py --dry-run

# Full migration
uv run migrate_adx_to_pg.py

# Resume
uv run migrate_adx_to_pg.py --resume

# Custom batch size
uv run migrate_adx_to_pg.py --resume --batch-size 2000
```

### How it works

1. Lists all distinct `(jobId, jobDateTime)` pairs from ADX.
2. For each job:
   - Queries `pricing_metrics | where jobId == '<id>'`.
   - Batch-inserts into `retail_prices_vm` with `ON CONFLICT DO NOTHING`.
   - Tracks progress in `job_runs` (dataset `adx_migration`).
3. Per-job failures are logged — the script continues with the next job.

---

## Verification

```sql
-- Total rows after migration
SELECT COUNT(*) FROM retail_prices_vm;

-- CSV pipeline job runs
SELECT run_id, status, items_read, items_written, started_at_utc, finished_at_utc,
       details->>'csv_file' AS csv_file
FROM job_runs
WHERE dataset = 'csv_migration'
ORDER BY started_at_utc;

-- Legacy migration job runs
SELECT run_id, status, items_read, items_written, started_at_utc, finished_at_utc,
       details->>'adx_job_id' AS adx_job_id
FROM job_runs
WHERE dataset = 'adx_migration'
ORDER BY started_at_utc;

-- Summary across both approaches
SELECT dataset, status, COUNT(*), SUM(items_read), SUM(items_written)
FROM job_runs
WHERE dataset IN ('csv_migration', 'adx_migration')
GROUP BY dataset, status;
```

## Environment variables

| Variable | Default | Description |
|---|---|---|
| `ADX_CLUSTER_URI` | `https://az-pricing-tool-adx.germanywestcentral.kusto.windows.net` | ADX cluster endpoint |
| `ADX_DATABASE_NAME` | `pricing-metrics` | ADX database |
| `ADX_TENANT_ID` | *(none)* | Azure tenant ID (for guest accounts) |
| `POSTGRES_HOST` | `localhost` | PG host |
| `POSTGRES_PORT` | `5432` | PG port |
| `POSTGRES_DB` | `azscout` | PG database |
| `POSTGRES_USER` | `azscout` | PG user |
| `POSTGRES_PASSWORD` | `azscout` | PG password |
| `POSTGRES_SSLMODE` | `disable` | PG SSL mode (`require` for Azure) |
