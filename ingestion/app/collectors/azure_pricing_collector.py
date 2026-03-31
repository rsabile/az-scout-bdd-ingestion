"""Azure Pricing Data Collector.

Collects retail pricing from the Azure Retail Prices API
(https://prices.azure.com/api/retail/prices) and ingests into PostgreSQL.

Uses a prefetch-thread pipeline: one background thread fetches API pages
while the main thread accumulates items and batch-inserts into PostgreSQL.
"""

import json
import logging
import queue
import threading
import time
from datetime import datetime
from typing import Any

import psycopg2  # type: ignore[import-untyped]
import requests
from psycopg2.extras import execute_values  # type: ignore[import-untyped]

from core.base_collector import BaseCollector

# Sentinel signaling the fetcher thread has finished producing pages.
_SENTINEL = object()


class AzurePricingCollector(BaseCollector):
    """Azure Retail Prices API data collector."""

    def __init__(
        self,
        job_id: str,
        job_datetime: datetime,
        job_type: str,
        config: dict[str, Any],
    ) -> None:
        super().__init__(job_id, job_datetime, job_type, config)

        # API configuration
        self.api_url = "https://prices.azure.com/api/retail/prices"
        self.api_retry_attempts = int(config.get("api_retry_attempts", 3))
        self.api_retry_delay = float(config.get("api_retry_delay", 2.0))

        # PG ingestion retry
        self.pg_retry_attempts = 5
        self.pg_retry_delay = 5  # seconds

        # Courtesy delay between API pages (seconds)
        self.page_delay = float(config.get("page_delay", 0.1))

        # Accumulate items before flushing to PG
        self.batch_accumulate_size = int(config.get("batch_accumulate_size", 2000))

        # Filters
        self.filters_json: str = config.get("filters_json", "{}")

        # Max items (-1 → unlimited)
        max_cfg = int(config.get("max_items", -1))
        self.max_items = float("inf") if max_cfg == -1 else max_cfg

        self.logger.info(
            "AzurePricingCollector init – max_items=%s, retry=%d/%s, "
            "page_delay=%.2fs, batch_accumulate=%d",
            "unlimited" if self.max_items == float("inf") else self.max_items,
            self.api_retry_attempts,
            self.api_retry_delay,
            self.page_delay,
            self.batch_accumulate_size,
        )

    # ------------------------------------------------------------------
    # Abstract property implementations
    # ------------------------------------------------------------------

    @property
    def collector_name(self) -> str:
        return "azure_pricing"

    @property
    def table_name(self) -> str:
        return "retail_prices_vm"

    @property
    def table_schema(self) -> str:
        return """
            CREATE TABLE IF NOT EXISTS retail_prices_vm (
                job_id              TEXT,
                job_datetime        TIMESTAMPTZ,
                job_type            TEXT,
                currency_code       TEXT NOT NULL,
                tier_minimum_units  NUMERIC,
                retail_price        NUMERIC,
                unit_price          NUMERIC,
                arm_region_name     TEXT NOT NULL,
                location            TEXT,
                effective_start_date TIMESTAMPTZ,
                meter_id            TEXT,
                meter_name          TEXT,
                product_id          TEXT,
                sku_id              TEXT,
                product_name        TEXT,
                sku_name            TEXT,
                service_name        TEXT,
                service_id          TEXT,
                service_family      TEXT,
                unit_of_measure     TEXT,
                pricing_type        TEXT,
                is_primary_meter_region BOOLEAN,
                arm_sku_name        TEXT,
                reservation_term    TEXT,
                savings_plan        JSONB,
                UNIQUE (currency_code, arm_region_name, sku_id, pricing_type, reservation_term, job_id)
            )
        """

    def validate_config(self) -> None:
        if self.filters_json and self.filters_json != "{}":
            try:
                json.loads(self.filters_json)
            except json.JSONDecodeError as exc:
                raise ValueError(f"Invalid AZURE_PRICING_FILTERS JSON: {exc}") from exc

    # ------------------------------------------------------------------
    # API helpers
    # ------------------------------------------------------------------

    def build_filter_params(self) -> dict[str, str]:
        """Convert AZURE_PRICING_FILTERS JSON into OData $filter query param."""
        # Preview API version required for savingsPlan field in responses
        params: dict[str, str] = {"api-version": "2023-01-01-preview"}

        if not self.filters_json or self.filters_json == "{}":
            return params

        try:
            filters: dict[str, Any] = json.loads(self.filters_json)
            if not filters:
                return params

            # currencyCode is a top-level query parameter, not an OData $filter field
            if "currencyCode" in filters:
                params["currencyCode"] = str(filters.pop("currencyCode"))
                self.logger.info("Using currencyCode: %s", params["currencyCode"])

            parts: list[str] = []
            for key, value in filters.items():
                if isinstance(value, str):
                    escaped = value.replace("'", "''")
                    parts.append(f"{key} eq '{escaped}'")
                elif isinstance(value, bool):
                    parts.append(f"{key} eq {str(value).lower()}")
                elif isinstance(value, (int, float)):
                    parts.append(f"{key} eq {value}")
                else:
                    self.logger.warning("Unsupported filter type for %s: %s", key, type(value))

            if parts:
                odata_filter = " and ".join(parts)
                params["$filter"] = odata_filter
                self.logger.info("Using OData filter: %s", odata_filter)

        except Exception:
            self.logger.exception("Error building filter parameters")

        return params

    def make_api_request(
        self,
        session: requests.Session,
        url: str,
        params: dict[str, str] | None = None,
    ) -> dict[str, Any]:
        """GET with retry logic (429 rate-limiting, 5xx, network errors)."""
        last_exc: Exception | None = None

        for attempt in range(self.api_retry_attempts):
            try:
                self.logger.debug("API request attempt %d/%d", attempt + 1, self.api_retry_attempts)
                resp = session.get(url, params=params) if params else session.get(url)

                if resp.status_code == 200:
                    return resp.json()  # type: ignore[no-any-return]

                if resp.status_code == 429:
                    retry_after = int(resp.headers.get("Retry-After", self.api_retry_delay * 2))
                    self.logger.warning("Rate limited (429), waiting %ds", retry_after)
                    time.sleep(retry_after)
                    last_exc = Exception(f"Rate limited (429) on attempt {attempt + 1}")
                    continue

                if 500 <= resp.status_code < 600:
                    self.logger.warning("Server error %d on attempt %d", resp.status_code, attempt + 1)
                    last_exc = Exception(f"Server error {resp.status_code}: {resp.text[:200]}")
                else:
                    raise Exception(
                        f"API request failed with status {resp.status_code}: {resp.text[:200]}"
                    )

            except requests.exceptions.RequestException as exc:
                self.logger.warning("Network error on attempt %d: %s", attempt + 1, exc)
                last_exc = exc

            if attempt < self.api_retry_attempts - 1:
                self.logger.info("Waiting %.1fs before retry…", self.api_retry_delay)
                time.sleep(self.api_retry_delay)

        raise Exception(
            f"API request failed after {self.api_retry_attempts} attempts: {last_exc}"
        )

    # ------------------------------------------------------------------
    # PostgreSQL ingestion
    # ------------------------------------------------------------------

    def ingest_batch_to_pg(
        self,
        pg_conn: Any,
        items: list[dict[str, Any]],
        batch_id: str,
    ) -> bool:
        """Upsert a batch of enriched items into PostgreSQL with retry."""
        if not items:
            return True

        last_exc: Exception | None = None

        for attempt in range(self.pg_retry_attempts):
            try:
                self.logger.debug(
                    "Ingesting batch %s – %d items (attempt %d/%d)",
                    batch_id,
                    len(items),
                    attempt + 1,
                    self.pg_retry_attempts,
                )

                with pg_conn.cursor() as cur:
                    rows = []
                    for item in items:
                        savings_plan = item.get("savingsPlan")
                        savings_plan_json = (
                            json.dumps(savings_plan) if savings_plan is not None else None
                        )

                        rows.append((
                            item.get("jobId"),
                            item.get("jobDateTime"),
                            item.get("jobType"),
                            item.get("currencyCode"),
                            item.get("tierMinimumUnits"),
                            item.get("retailPrice"),
                            item.get("unitPrice"),
                            item.get("armRegionName"),
                            item.get("location"),
                            item.get("effectiveStartDate"),
                            item.get("meterId"),
                            item.get("meterName"),
                            item.get("productId"),
                            item.get("skuId"),
                            item.get("productName"),
                            item.get("skuName"),
                            item.get("serviceName"),
                            item.get("serviceId"),
                            item.get("serviceFamily"),
                            item.get("unitOfMeasure"),
                            item.get("type"),
                            item.get("isPrimaryMeterRegion"),
                            item.get("armSkuName"),
                            item.get("reservationTerm"),
                            savings_plan_json,
                        ))

                    execute_values(
                        cur,
                        """
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
                        ) VALUES %s
                        ON CONFLICT (currency_code, arm_region_name, sku_id, pricing_type, reservation_term, job_id)
                        DO NOTHING
                        """,
                        rows,
                        page_size=1000,
                    )

                pg_conn.commit()

                if attempt > 0:
                    self.logger.info(
                        "Batch %s ingested after %d attempts", batch_id, attempt + 1
                    )
                else:
                    self.logger.debug("Batch %s ingested successfully", batch_id)
                return True

            except psycopg2.Error as exc:
                pg_conn.rollback()
                last_exc = exc
                self.logger.warning(
                    "PG error batch %s attempt %d/%d: %s",
                    batch_id,
                    attempt + 1,
                    self.pg_retry_attempts,
                    exc,
                )
                if attempt < self.pg_retry_attempts - 1:
                    time.sleep(self.pg_retry_delay)

            except Exception as exc:
                pg_conn.rollback()
                last_exc = exc
                self.logger.warning(
                    "Ingestion error batch %s attempt %d/%d: %s",
                    batch_id,
                    attempt + 1,
                    self.pg_retry_attempts,
                    exc,
                )
                if attempt < self.pg_retry_attempts - 1:
                    time.sleep(self.pg_retry_delay)

        self.logger.error(
            "Failed to ingest batch %s after %d attempts: %s",
            batch_id,
            self.pg_retry_attempts,
            last_exc,
        )
        return False

    # ------------------------------------------------------------------
    # Main collection loop
    # ------------------------------------------------------------------

    def _fetcher_thread(
        self,
        page_queue: queue.Queue[Any],
        filter_params: dict[str, str],
    ) -> None:
        """Background thread that fetches API pages and pushes them to *page_queue*.

        Produces ``(page_number, items_list)`` tuples.  On completion (or
        error) it pushes the ``_SENTINEL`` so the consumer knows to stop.
        """
        logger = logging.getLogger(f"{__name__}.fetcher")
        next_page_link: str | None = self.api_url
        page_count = 0

        # Each thread needs its own Session (requests.Session is not
        # guaranteed thread-safe for *concurrent* use).
        session = requests.Session()
        session.timeout = 300  # 5 min

        try:
            while next_page_link:
                page_count += 1
                logger.debug("Fetcher – page %d", page_count)

                if page_count == 1:
                    data = self.make_api_request(session, next_page_link, filter_params)
                else:
                    data = self.make_api_request(session, next_page_link)

                items: list[dict[str, Any]] = data.get("Items", [])
                if not items:
                    logger.info("Fetcher – no more items after page %d", page_count)
                    break

                page_queue.put((page_count, items))

                next_page_link = data.get("NextPageLink")
                if next_page_link and self.page_delay > 0:
                    time.sleep(self.page_delay)
        except Exception:
            logger.exception("Fetcher thread error on page %d", page_count)
            raise

    def collect_data(self, pg_conn: Any) -> int:
        """Pipelined collection: prefetch API pages in a background thread
        while the main thread accumulates and batch-inserts into PostgreSQL."""
        total_items = 0
        total_ingested = 0
        batch_number = 0
        buffer: list[dict[str, Any]] = []

        self.logger.info("Starting pipelined pricing data collection and ingestion")

        filter_params = self.build_filter_params()
        page_queue: queue.Queue[Any] = queue.Queue(maxsize=5)

        # Start fetcher in a daemon thread so it doesn't block shutdown.
        fetcher_error: list[BaseException] = []

        def _fetcher_wrapper() -> None:
            try:
                self._fetcher_thread(page_queue, filter_params)
            except BaseException as exc:
                fetcher_error.append(exc)
            finally:
                # Always place sentinel so the consumer unblocks.
                try:
                    page_queue.put(_SENTINEL)
                except Exception:
                    pass

        thread = threading.Thread(target=_fetcher_wrapper, daemon=True)
        thread.start()

        # ---- consumer (main thread) ----
        while True:
            entry = page_queue.get()
            if entry is _SENTINEL:
                break

            page_number, items = entry

            for item in items:
                if total_items >= self.max_items:
                    self.logger.info("Reached max_items limit (%s)", self.max_items)
                    break

                enriched = self.enrich_item(item)
                buffer.append(enriched)
                total_items += 1

            # Flush when buffer reaches accumulate threshold
            if len(buffer) >= self.batch_accumulate_size:
                batch_number += 1
                ok = self.ingest_batch_to_pg(pg_conn, buffer, f"batch-{batch_number}")
                if ok:
                    total_ingested += len(buffer)
                    self.logger.info(
                        "Batch %d: ingested %d items (total: %d)",
                        batch_number,
                        len(buffer),
                        total_ingested,
                    )
                else:
                    raise Exception(
                        f"Failed to ingest batch {batch_number} ({len(buffer)} items)"
                    )
                buffer.clear()

            if total_items >= self.max_items:
                break

        # Flush remaining items
        if buffer:
            batch_number += 1
            ok = self.ingest_batch_to_pg(pg_conn, buffer, f"batch-{batch_number}")
            if ok:
                total_ingested += len(buffer)
                self.logger.info(
                    "Final batch %d: ingested %d items (total: %d)",
                    batch_number,
                    len(buffer),
                    total_ingested,
                )
            else:
                raise Exception(
                    f"Failed to ingest final batch {batch_number} ({len(buffer)} items)"
                )
            buffer.clear()

        # Wait for the fetcher thread to finish (should already be done).
        thread.join(timeout=30)

        if fetcher_error:
            raise Exception(
                f"Fetcher thread failed: {fetcher_error[0]}"
            ) from fetcher_error[0]

        self.total_collected = total_items
        self.total_ingested = total_ingested

        self.logger.info(
            "Collection completed: %d items ingested in %d batches",
            total_ingested,
            batch_number,
        )
        return total_ingested
