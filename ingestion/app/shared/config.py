"""Configuration Manager.

Centralised configuration management for the ingestion system.
Handles environment variables, .env.local loading, validation,
and collector-specific configuration extraction.
"""

import json
import logging
import os
from pathlib import Path
from typing import Any


class ConfigManager:
    """Centralised configuration management."""

    def __init__(self) -> None:
        self.logger = logging.getLogger(__name__)
        self._load_env_file()
        self._config = self._load_config()

    # ------------------------------------------------------------------
    # .env.local loader
    # ------------------------------------------------------------------

    def _load_env_file(self) -> None:
        """Load environment variables from a .env.local file (if found)."""
        possible_locations = [
            Path.cwd() / ".env.local",
            Path.cwd().parent / ".env.local",
            Path(__file__).resolve().parent.parent.parent / ".env.local",
        ]

        env_file: Path | None = None
        for loc in possible_locations:
            if loc.exists():
                env_file = loc
                break

        if env_file is None:
            self.logger.debug("No .env.local file found in standard locations")
            return

        self.logger.debug("Loading environment from: %s", env_file)
        try:
            with open(env_file) as fh:
                for line_num, line in enumerate(fh, 1):
                    line = line.strip()
                    if not line or line.startswith("#") or "=" not in line:
                        continue
                    try:
                        key, value = line.split("=", 1)
                        key = key.strip()
                        value = value.strip()
                        # Strip surrounding quotes
                        if len(value) >= 2 and value[0] == value[-1] and value[0] in ('"', "'"):
                            value = value[1:-1]
                        # Env vars take precedence over file values
                        if key not in os.environ:
                            os.environ[key] = value
                    except ValueError:
                        self.logger.warning("Invalid line %d in %s: %s", line_num, env_file, line)
        except Exception:
            self.logger.warning("Error loading %s", env_file, exc_info=True)

    # ------------------------------------------------------------------
    # Configuration loading
    # ------------------------------------------------------------------

    def _load_config(self) -> dict[str, Any]:
        return {
            # PostgreSQL
            "postgres_host": os.getenv("POSTGRES_HOST", "localhost"),
            "postgres_port": os.getenv("POSTGRES_PORT", "5432"),
            "postgres_db": os.getenv("POSTGRES_DB", "azscout"),
            "postgres_user": os.getenv("POSTGRES_USER", "azscout"),
            "postgres_password": os.getenv("POSTGRES_PASSWORD", "azscout"),
            "postgres_sslmode": os.getenv("POSTGRES_SSLMODE", "disable"),
            # Job
            "job_type": os.getenv("JOB_TYPE", "manual"),
            "environment": os.getenv("ENVIRONMENT", "production"),
            # Azure Pricing Collector
            "enable_azure_pricing_collector": os.getenv(
                "ENABLE_AZURE_PRICING_COLLECTOR", "false"
            ).lower()
            == "true",
            "azure_pricing_max_items": os.getenv("AZURE_PRICING_MAX_ITEMS", "-1"),
            "azure_pricing_api_retry_attempts": os.getenv("AZURE_PRICING_API_RETRY_ATTEMPTS", "3"),
            "azure_pricing_api_retry_delay": os.getenv("AZURE_PRICING_API_RETRY_DELAY", "2.0"),
            "azure_pricing_filters": os.getenv("AZURE_PRICING_FILTERS", "{}"),
            "azure_pricing_page_delay": os.getenv("AZURE_PRICING_PAGE_DELAY", "0.1"),
            "azure_pricing_batch_accumulate_size": os.getenv(
                "AZURE_PRICING_BATCH_ACCUMULATE_SIZE", "2000"
            ),
            # Azure Spot Collector
            "enable_azure_spot_collector": os.getenv(
                "ENABLE_AZURE_SPOT_COLLECTOR", "false"
            ).lower()
            == "true",
            "azure_spot_eviction_only": os.getenv(
                "AZURE_SPOT_EVICTION_ONLY", "false"
            ).lower()
            == "true",
            "azure_spot_max_items": os.getenv("AZURE_SPOT_MAX_ITEMS", "-1"),
            "azure_spot_api_retry_attempts": os.getenv("AZURE_SPOT_API_RETRY_ATTEMPTS", "3"),
            "azure_spot_api_retry_delay": os.getenv("AZURE_SPOT_API_RETRY_DELAY", "2.0"),
            # Logging
            "log_level": os.getenv("LOG_LEVEL", "INFO"),
        }

    # ------------------------------------------------------------------
    # Accessors
    # ------------------------------------------------------------------

    def get_global_config(self) -> dict[str, Any]:
        return {
            "postgres_host": self._config["postgres_host"],
            "postgres_port": self._config["postgres_port"],
            "postgres_db": self._config["postgres_db"],
            "postgres_user": self._config["postgres_user"],
            "postgres_password": self._config["postgres_password"],
            "postgres_sslmode": self._config["postgres_sslmode"],
            "job_type": self._config["job_type"],
            "environment": self._config["environment"],
            "log_level": self._config["log_level"],
        }

    def get_collector_config(self, collector_name: str) -> dict[str, Any]:
        config = self.get_global_config()

        if collector_name == "azure_pricing":
            config.update(
                {
                    "api_retry_attempts": self._config["azure_pricing_api_retry_attempts"],
                    "api_retry_delay": self._config["azure_pricing_api_retry_delay"],
                    "max_items": self.get_int("azure_pricing_max_items", -1),
                    "filters_json": self.get_json("azure_pricing_filters", "{}"),
                    "page_delay": self._config["azure_pricing_page_delay"],
                    "batch_accumulate_size": self._config[
                        "azure_pricing_batch_accumulate_size"
                    ],
                }
            )
        elif collector_name == "azure_spot":
            config.update(
                {
                    "api_retry_attempts": self._config["azure_spot_api_retry_attempts"],
                    "api_retry_delay": self._config["azure_spot_api_retry_delay"],
                    "max_items": self.get_int("azure_spot_max_items", -1),
                    "eviction_only": self._config["azure_spot_eviction_only"],
                }
            )

        return config

    def validate_global_config(self) -> None:
        required = ["postgres_host", "postgres_db", "postgres_user"]
        for key in required:
            if not self._config.get(key):
                raise ValueError(f"Required configuration '{key}' is missing")

        valid_levels = {"DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"}
        if self._config["log_level"].upper() not in valid_levels:
            raise ValueError(f"Invalid log_level: {self._config['log_level']}")

    def get_collectors_to_run(self) -> list[str]:
        collectors: list[str] = []
        if self.get_bool("enable_azure_pricing_collector", default=False):
            collectors.append("azure_pricing")
        if self.get_bool("enable_azure_spot_collector", default=False):
            collectors.append("azure_spot")
        return collectors

    # ------------------------------------------------------------------
    # Type helpers
    # ------------------------------------------------------------------

    def get_value(self, key: str, default: Any = None) -> Any:
        return self._config.get(key, default)

    def get_int(self, key: str, default: int = 0) -> int:
        value = self._config.get(key, default)
        try:
            return int(value)  # type: ignore[arg-type]
        except (ValueError, TypeError):
            self.logger.warning("Invalid integer for %s: %s, using default %s", key, value, default)
            return default

    def get_float(self, key: str, default: float = 0.0) -> float:
        value = self._config.get(key, default)
        try:
            return float(value)  # type: ignore[arg-type]
        except (ValueError, TypeError):
            self.logger.warning("Invalid float for %s: %s, using default %s", key, value, default)
            return default

    def get_bool(self, key: str, default: bool = False) -> bool:
        value = self._config.get(key, default)
        if isinstance(value, bool):
            return value
        if isinstance(value, str):
            return value.lower() in ("true", "1", "yes", "on")
        return default

    def get_json(self, key: str, default: str = "{}") -> str:
        value = self._config.get(key, "{}")
        if isinstance(value, str):
            # Strip surrounding quotes
            if len(value) >= 2 and value[0] == value[-1] and value[0] in ('"', "'"):
                value = value[1:-1]
        try:
            json.loads(value)  # validate
            return value
        except (json.JSONDecodeError, TypeError):
            self.logger.warning("Invalid JSON for %s, using default", key)
            return default

    def log_diagnostics(self) -> None:
        self.logger.info("=== Configuration Diagnostics ===")
        diagnostic_keys = [
            "postgres_host",
            "postgres_db",
            "job_type",
            "environment",
        ]
        for key in diagnostic_keys:
            self.logger.info("%s: %s", key, self._config.get(key))
