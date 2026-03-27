"""SQL DDL and aggregation queries for the price aggregator job."""

# -- DDL -----------------------------------------------------------------------

CREATE_PRICE_SUMMARY = """\
CREATE TABLE IF NOT EXISTS price_summary (
    id              BIGSERIAL PRIMARY KEY,
    run_id          UUID NOT NULL,
    snapshot_utc    TIMESTAMPTZ NOT NULL,
    region          TEXT NOT NULL,
    category        TEXT,
    price_type      TEXT NOT NULL,
    currency_code   TEXT NOT NULL DEFAULT 'USD',
    avg_price       NUMERIC NOT NULL,
    median_price    NUMERIC NOT NULL,
    min_price       NUMERIC NOT NULL,
    max_price       NUMERIC NOT NULL,
    p10_price       NUMERIC NOT NULL,
    p25_price       NUMERIC NOT NULL,
    p75_price       NUMERIC NOT NULL,
    p90_price       NUMERIC NOT NULL,
    sku_count       INTEGER NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_price_summary_region
    ON price_summary (region);
CREATE INDEX IF NOT EXISTS idx_price_summary_category
    ON price_summary (category);
CREATE INDEX IF NOT EXISTS idx_price_summary_snapshot
    ON price_summary (snapshot_utc DESC);
CREATE INDEX IF NOT EXISTS idx_price_summary_run
    ON price_summary (run_id);
CREATE INDEX IF NOT EXISTS idx_price_summary_type
    ON price_summary (price_type);
"""

# -- Aggregation queries -------------------------------------------------------
# Each query INSERT ... SELECT pre-computed stats into price_summary.
# Uses PERCENTILE_CONT() WITHIN GROUP for percentiles (PostgreSQL aggregate).

AGGREGATE_RETAIL = """\
INSERT INTO price_summary (
    run_id, snapshot_utc, region, category, price_type, currency_code,
    avg_price, median_price, min_price, max_price,
    p10_price, p25_price, p75_price, p90_price, sku_count
)
SELECT
    %(run_id)s,
    NOW(),
    r.arm_region_name,
    c.category,
    'retail',
    r.currency_code,
    AVG(r.unit_price),
    PERCENTILE_CONT(0.5)  WITHIN GROUP (ORDER BY r.unit_price),
    MIN(r.unit_price),
    MAX(r.unit_price),
    PERCENTILE_CONT(0.10) WITHIN GROUP (ORDER BY r.unit_price),
    PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY r.unit_price),
    PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY r.unit_price),
    PERCENTILE_CONT(0.90) WITHIN GROUP (ORDER BY r.unit_price),
    COUNT(DISTINCT r.arm_sku_name)
FROM retail_prices_vm r
JOIN vm_sku_catalog c ON r.arm_sku_name = c.sku_name
WHERE r.pricing_type  = 'Consumption'
  AND r.sku_name NOT LIKE '%% Spot%%'
  AND r.unit_price > 0
GROUP BY GROUPING SETS (
    (r.arm_region_name, c.category, r.currency_code),
    (r.arm_region_name, r.currency_code)
);
"""

AGGREGATE_SPOT = """\
INSERT INTO price_summary (
    run_id, snapshot_utc, region, category, price_type, currency_code,
    avg_price, median_price, min_price, max_price,
    p10_price, p25_price, p75_price, p90_price, sku_count
)
SELECT
    %(run_id)s,
    NOW(),
    r.arm_region_name,
    c.category,
    'spot',
    r.currency_code,
    AVG(r.unit_price),
    PERCENTILE_CONT(0.5)  WITHIN GROUP (ORDER BY r.unit_price),
    MIN(r.unit_price),
    MAX(r.unit_price),
    PERCENTILE_CONT(0.10) WITHIN GROUP (ORDER BY r.unit_price),
    PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY r.unit_price),
    PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY r.unit_price),
    PERCENTILE_CONT(0.90) WITHIN GROUP (ORDER BY r.unit_price),
    COUNT(DISTINCT r.arm_sku_name)
FROM retail_prices_vm r
JOIN vm_sku_catalog c ON r.arm_sku_name = c.sku_name
WHERE r.pricing_type  = 'Consumption'
  AND r.sku_name LIKE '%% Spot%%'
  AND r.unit_price > 0
GROUP BY GROUPING SETS (
    (r.arm_region_name, c.category, r.currency_code),
    (r.arm_region_name, r.currency_code)
);
"""

# Ordered list for sequential execution.
AGGREGATION_QUERIES: list[tuple[str, str]] = [
    ("retail", AGGREGATE_RETAIL),
    ("spot", AGGREGATE_SPOT),
]

# -- Job tracking (reuses job_runs table from ingestion) -----------------------

INSERT_JOB_RUN = """\
INSERT INTO job_runs (run_id, dataset, status, started_at_utc)
VALUES (%(run_id)s, %(dataset)s, 'running', NOW());
"""

UPDATE_JOB_RUN_OK = """\
UPDATE job_runs
   SET status         = 'ok',
       finished_at_utc = NOW(),
       items_read      = %(items_read)s,
       items_written   = %(items_written)s
 WHERE run_id = %(run_id)s;
"""

UPDATE_JOB_RUN_ERROR = """\
UPDATE job_runs
   SET status         = 'error',
       finished_at_utc = NOW(),
       error_message   = %(error_message)s
 WHERE run_id = %(run_id)s;
"""

# -- Prerequisite check --------------------------------------------------------

COUNT_SKU_CATALOG = """\
SELECT COUNT(*) FROM vm_sku_catalog;
"""
