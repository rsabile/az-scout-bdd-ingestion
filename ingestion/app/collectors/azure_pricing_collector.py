"""Azure Pricing Data Collector.

Collects VM retail pricing from the Azure Retail Prices API
(https://prices.azure.com/api/retail/prices) and ingests into PostgreSQL.

Mirrors the collector pattern from az-pricing-history but stores data
in a PostgreSQL table instead of Azure Data Explorer.
"""

import json
import time
from datetime import datetime
from typing import Any

import psycopg2  # type: ignore[import-untyped]
import requests
from core.base_collector import BaseCollector


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

        # Filters
        self.filters_json: str = config.get("filters_json", "{}")

        # Max items (-1 → unlimited)
        max_cfg = int(config.get("max_items", -1))
        self.max_items = float("inf") if max_cfg == -1 else max_cfg

        self.logger.info(
            "AzurePricingCollector init – max_items=%s, retry=%d/%s",
            "unlimited" if self.max_items == float("inf") else self.max_items,
            self.api_retry_attempts,
            self.api_retry_delay,
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
                    for item in items:
                        savings_plan = item.get("savingsPlan")
                        savings_plan_json = (
                            json.dumps(savings_plan) if savings_plan is not None else None
                        )

                        params = {
                            "jobId": item.get("jobId"),
                            "jobDateTime": item.get("jobDateTime"),
                            "jobType": item.get("jobType"),
                            "currencyCode": item.get("currencyCode"),
                            "tierMinimumUnits": item.get("tierMinimumUnits"),
                            "retailPrice": item.get("retailPrice"),
                            "unitPrice": item.get("unitPrice"),
                            "armRegionName": item.get("armRegionName"),
                            "location": item.get("location"),
                            "effectiveStartDate": item.get("effectiveStartDate"),
                            "meterId": item.get("meterId"),
                            "meterName": item.get("meterName"),
                            "productId": item.get("productId"),
                            "skuId": item.get("skuId"),
                            "productName": item.get("productName"),
                            "skuName": item.get("skuName"),
                            "serviceName": item.get("serviceName"),
                            "serviceId": item.get("serviceId"),
                            "serviceFamily": item.get("serviceFamily"),
                            "unitOfMeasure": item.get("unitOfMeasure"),
                            "type": item.get("type"),
                            "isPrimaryMeterRegion": item.get("isPrimaryMeterRegion"),
                            "armSkuName": item.get("armSkuName"),
                            "reservationTerm": item.get("reservationTerm"),
                            "savingsPlanJson": savings_plan_json,
                        }

                        cur.execute(
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
                            ) VALUES (
                                %(jobId)s, %(jobDateTime)s, %(jobType)s,
                                %(currencyCode)s, %(tierMinimumUnits)s,
                                %(retailPrice)s, %(unitPrice)s,
                                %(armRegionName)s, %(location)s,
                                %(effectiveStartDate)s,
                                %(meterId)s, %(meterName)s,
                                %(productId)s, %(skuId)s, %(productName)s, %(skuName)s,
                                %(serviceName)s, %(serviceId)s, %(serviceFamily)s,
                                %(unitOfMeasure)s, %(type)s,
                                %(isPrimaryMeterRegion)s, %(armSkuName)s,
                                %(reservationTerm)s, %(savingsPlanJson)s
                            )
                            ON CONFLICT (currency_code, arm_region_name, sku_id, pricing_type, reservation_term, job_id)
                            DO NOTHING
                            """,
                            params,
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

    def collect_data(self, pg_conn: Any) -> int:
        """Fetch pricing data page-by-page and ingest into PostgreSQL."""
        total_items = 0
        total_ingested = 0

        self.logger.info("Starting real-time pricing data collection and ingestion")

        filter_params = self.build_filter_params()
        next_page_link: str | None = self.api_url
        page_count = 0

        session = requests.Session()
        session.timeout = 300  # 5 min

        if filter_params:
            self.logger.info("Starting collection from: %s with filters", self.api_url)
        else:
            self.logger.info("Starting collection from: %s (no filters)", self.api_url)

        while next_page_link and total_items < self.max_items:
            page_count += 1
            self.logger.info("Fetching page %d …", page_count)

            if page_count == 1:
                data = self.make_api_request(session, next_page_link, filter_params)
            else:
                data = self.make_api_request(session, next_page_link)

            items: list[dict[str, Any]] = data.get("Items", [])
            if not items:
                self.logger.info("No more items – stopping pagination")
                break

            page_items: list[dict[str, Any]] = []
            for item in items:
                if total_items >= self.max_items:
                    self.logger.info("Reached max_items limit (%s)", self.max_items)
                    break

                enriched = self.enrich_item(item)
                page_items.append(enriched)
                total_items += 1

            if page_items:
                ok = self.ingest_batch_to_pg(pg_conn, page_items, f"page-{page_count}")
                if ok:
                    total_ingested += len(page_items)
                    self.logger.info(
                        "Page %d: ingested %d items (total: %d)",
                        page_count,
                        len(page_items),
                        total_ingested,
                    )
                else:
                    raise Exception(
                        f"Failed to ingest page {page_count} ({len(page_items)} items)"
                    )

            page_items.clear()

            if total_items >= self.max_items:
                break

            next_page_link = data.get("NextPageLink")

            # Rate-limit courtesy delay
            time.sleep(1)

        self.total_collected = total_items
        self.total_ingested = total_ingested

        self.logger.info("Collection completed: %d items ingested", total_ingested)
        return total_ingested
