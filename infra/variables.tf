variable "subscription_id" {
  description = "Azure subscription ID"
  type        = string
}

variable "resource_group_name" {
  description = "Name of the Azure resource group"
  type        = string
  default     = "rg-azure-scout-bdd"
}

variable "location" {
  description = "Azure region for resources"
  type        = string
  default     = "swedencentral"
}

variable "prefix" {
  description = "Prefix for resource names"
  type        = string
  default     = "az-scout"
}

variable "environment" {
  description = "Environment tag"
  type        = string
  default     = "production"
}

# ---------------------------------------------------------------------
# PostgreSQL
# ---------------------------------------------------------------------

variable "postgres_admin_user" {
  description = "PostgreSQL administrator login"
  type        = string
  default     = "azscout"
}

variable "postgres_admin_password" {
  description = "PostgreSQL administrator password"
  type        = string
  sensitive   = true
}

variable "postgres_db_name" {
  description = "PostgreSQL database name"
  type        = string
  default     = "azscout"
}

variable "postgres_sku" {
  description = "PostgreSQL Flexible Server SKU (e.g. B_Standard_B1ms, GP_Standard_D2s_v3)"
  type        = string
  default     = "B_Standard_B1ms"
}

variable "postgres_storage_mb" {
  description = "PostgreSQL storage in MB"
  type        = number
  default     = 32768 # 32 GB
}

# ---------------------------------------------------------------------
# Ingestion Jobs
# ---------------------------------------------------------------------

variable "max_pricing_items" {
  description = "Maximum pricing items to collect per job (-1 = unlimited)"
  type        = string
  default     = "-1"
}

variable "enable_spot_collector" {
  description = "Enable the Azure Spot pricing & eviction collector"
  type        = bool
  default     = true
}

variable "max_spot_items" {
  description = "Maximum spot items to collect per job (-1 = unlimited)"
  type        = string
  default     = "-1"
}

variable "cron_expression" {
  description = "Cron expression for the main scheduled ingestion job (UTC)"
  type        = string
  default     = "0 2 * * *" # Daily at 02:00 UTC
}

variable "spot_eviction_cron" {
  description = "Cron expression for the hourly spot eviction historization job (UTC)"
  type        = string
  default     = "0 * * * *" # Every hour
}

variable "log_level" {
  description = "Log level for ingestion jobs"
  type        = string
  default     = "INFO"
}

variable "sku_mapper_cron" {
  description = "Cron expression for the SKU mapper job (UTC). Should run after ingestion."
  type        = string
  default     = "0 4 * * *" # Daily at 04:00 UTC
}

variable "price_aggregator_cron" {
  description = "Cron expression for the price aggregator job (UTC). Should run after all currency pricing jobs."
  type        = string
  default     = "0 7 * * *" # Daily at 07:00 UTC
}

# ---------------------------------------------------------------------
# Multi-Currency Pricing Jobs
# ---------------------------------------------------------------------

variable "currency_codes" {
  description = "List of currency codes to collect pricing data for. One Container App Job is created per currency."
  type        = list(string)
  default = [
    "USD", "EUR", "GBP", "JPY", "AUD", "CAD", "CHF", "CNY",
    "DKK", "INR", "KRW", "NOK", "NZD", "SEK", "BRL", "MXN",
    "ZAR", "ARS", "RUB", "TWD", "MYR", "IDR", "TRY", "HKD",
    "SAR", "ISK"
  ]
}

variable "currency_stagger_minutes" {
  description = "Minutes between each currency job start to limit concurrent PG connections"
  type        = number
  default     = 5
}

variable "currency_start_hour" {
  description = "UTC hour for the first currency pricing job"
  type        = number
  default     = 1
}

variable "currency_job_timeout" {
  description = "Replica timeout in seconds for each currency pricing job"
  type        = number
  default     = 21600 # 6h
}

# ---------------------------------------------------------------------
# API Container App
# ---------------------------------------------------------------------

variable "api_cpu" {
  description = "CPU cores for the API container (e.g. 0.25, 0.5, 1.0)"
  type        = number
  default     = 1.0
}

variable "api_memory" {
  description = "Memory for the API container (e.g. 1Gi, 2Gi)"
  type        = string
  default     = "2Gi"
}

variable "api_min_replicas" {
  description = "Minimum number of API replicas (>=1 for always-on)"
  type        = number
  default     = 1
}

variable "api_max_replicas" {
  description = "Maximum number of API replicas for auto-scaling"
  type        = number
  default     = 10
}

variable "api_port" {
  description = "Port exposed by the API container"
  type        = number
  default     = 8000
}
