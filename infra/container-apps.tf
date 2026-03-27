# Container Apps Infrastructure for Ingestion Jobs
# ACR + Container Apps Environment + Scheduled & Manual Jobs

# ---------------------------------------------------------------------
# Managed Identity for Container Apps Jobs
# ---------------------------------------------------------------------
resource "azurerm_user_assigned_identity" "ingestion_jobs" {
  name                = "${var.prefix}-jobs-id"
  location            = azurerm_resource_group.main.location
  resource_group_name = azurerm_resource_group.main.name

  tags = {
    Environment = var.environment
    Project     = "bdd-sku"
  }
}

# ---------------------------------------------------------------------
# Log Analytics
# ---------------------------------------------------------------------
resource "azurerm_log_analytics_workspace" "main" {
  name                = "${var.prefix}-law"
  location            = azurerm_resource_group.main.location
  resource_group_name = azurerm_resource_group.main.name
  sku                 = "PerGB2018"
  retention_in_days   = 30

  tags = {
    Environment = var.environment
    Project     = "bdd-sku"
  }
}

# ---------------------------------------------------------------------
# Container Apps Environment
# ---------------------------------------------------------------------
resource "azurerm_container_app_environment" "main" {
  name                       = "${var.prefix}-cae"
  location                   = azurerm_resource_group.main.location
  resource_group_name        = azurerm_resource_group.main.name
  log_analytics_workspace_id = azurerm_log_analytics_workspace.main.id

  tags = {
    Environment = var.environment
    Project     = "bdd-sku"
  }
}

# Diagnostic settings
resource "azurerm_monitor_diagnostic_setting" "cae" {
  name                       = "${var.prefix}-cae-diag"
  target_resource_id         = azurerm_container_app_environment.main.id
  log_analytics_workspace_id = azurerm_log_analytics_workspace.main.id

  enabled_log {
    category = "ContainerAppConsoleLogs"
  }

  enabled_log {
    category = "ContainerAppSystemLogs"
  }

  enabled_metric {
    category = "AllMetrics"
  }
}

# ---------------------------------------------------------------------
# Azure Container Registry
# ---------------------------------------------------------------------
resource "azurerm_container_registry" "main" {
  name                = "${replace(var.prefix, "-", "")}acr"
  resource_group_name = azurerm_resource_group.main.name
  location            = azurerm_resource_group.main.location
  sku                 = "Basic"
  admin_enabled       = false

  identity {
    type = "SystemAssigned"
  }

  tags = {
    Environment = var.environment
    Project     = "bdd-sku"
  }
}

# ACR Pull for managed identity
resource "azurerm_role_assignment" "ingestion_acr_pull" {
  scope                = azurerm_container_registry.main.id
  role_definition_name = "AcrPull"
  principal_id         = azurerm_user_assigned_identity.ingestion_jobs.principal_id
}

# Contributor on the subscription
resource "azurerm_role_assignment" "ingestion_contributor" {
  scope                = "/subscriptions/${var.subscription_id}"
  role_definition_name = "Contributor"
  principal_id         = azurerm_user_assigned_identity.ingestion_jobs.principal_id
}

# ---------------------------------------------------------------------
# Multi-Currency Pricing Jobs – staggered schedule
# ---------------------------------------------------------------------

locals {
  currency_schedule = {
    for idx, code in var.currency_codes : code => {
      minute = (idx * var.currency_stagger_minutes) % 60
      hour   = var.currency_start_hour + floor((idx * var.currency_stagger_minutes) / 60)
    }
  }
}

resource "azurerm_container_app_job" "pricing_currency" {
  for_each = local.currency_schedule

  name                         = "${var.prefix}-price-${lower(each.key)}"
  location                     = azurerm_resource_group.main.location
  resource_group_name          = azurerm_resource_group.main.name
  container_app_environment_id = azurerm_container_app_environment.main.id

  identity {
    type         = "UserAssigned"
    identity_ids = [azurerm_user_assigned_identity.ingestion_jobs.id]
  }

  replica_timeout_in_seconds = var.currency_job_timeout
  replica_retry_limit        = 3

  schedule_trigger_config {
    cron_expression          = "${each.value.minute} ${each.value.hour} * * *"
    parallelism              = 1
    replica_completion_count = 1
  }

  template {
    container {
      name   = "bdd-sku-ingestion"
      image  = "${azurerm_container_registry.main.login_server}/bdd-sku-ingestion:latest"
      cpu    = 1.0
      memory = "2Gi"

      env {
        name  = "POSTGRES_HOST"
        value = azurerm_postgresql_flexible_server.main.fqdn
      }

      env {
        name  = "POSTGRES_PORT"
        value = "5432"
      }

      env {
        name  = "POSTGRES_DB"
        value = var.postgres_db_name
      }

      env {
        name  = "POSTGRES_USER"
        value = var.postgres_admin_user
      }

      env {
        name        = "POSTGRES_PASSWORD"
        secret_name = "pg-password"
      }

      env {
        name  = "POSTGRES_SSLMODE"
        value = "require"
      }

      env {
        name  = "ENABLE_AZURE_PRICING_COLLECTOR"
        value = "true"
      }

      env {
        name  = "AZURE_PRICING_MAX_ITEMS"
        value = var.max_pricing_items
      }

      env {
        name  = "AZURE_PRICING_FILTERS"
        value = jsonencode({ "currencyCode" = each.key })
      }

      env {
        name  = "ENABLE_AZURE_SPOT_COLLECTOR"
        value = "false"
      }

      env {
        name  = "JOB_TYPE"
        value = "scheduled-pricing-${lower(each.key)}"
      }

      env {
        name  = "PYTHONUNBUFFERED"
        value = "1"
      }

      env {
        name  = "LOG_LEVEL"
        value = var.log_level
      }

      env {
        name  = "AZURE_CLIENT_ID"
        value = azurerm_user_assigned_identity.ingestion_jobs.client_id
      }
    }
  }

  secret {
    name  = "pg-password"
    value = var.postgres_admin_password
  }

  registry {
    server   = azurerm_container_registry.main.login_server
    identity = azurerm_user_assigned_identity.ingestion_jobs.id
  }

  tags = {
    Environment  = var.environment
    Project      = "bdd-sku"
    JobType      = "pricing-currency"
    CurrencyCode = each.key
  }
}

# ---------------------------------------------------------------------
# Container Apps Job – Scheduled Spot Collection (daily at 02:00 UTC)
# ---------------------------------------------------------------------
resource "azurerm_container_app_job" "ingestion_scheduled" {
  name                         = "${var.prefix}-sched"
  location                     = azurerm_resource_group.main.location
  resource_group_name          = azurerm_resource_group.main.name
  container_app_environment_id = azurerm_container_app_environment.main.id

  identity {
    type         = "UserAssigned"
    identity_ids = [azurerm_user_assigned_identity.ingestion_jobs.id]
  }

  replica_timeout_in_seconds = 3600
  replica_retry_limit        = 3

  schedule_trigger_config {
    cron_expression          = var.cron_expression
    parallelism              = 1
    replica_completion_count = 1
  }

  template {
    container {
      name   = "bdd-sku-ingestion"
      image  = "${azurerm_container_registry.main.login_server}/bdd-sku-ingestion:latest"
      cpu    = 1.0
      memory = "2Gi"

      env {
        name  = "POSTGRES_HOST"
        value = azurerm_postgresql_flexible_server.main.fqdn
      }

      env {
        name  = "POSTGRES_PORT"
        value = "5432"
      }

      env {
        name  = "POSTGRES_DB"
        value = var.postgres_db_name
      }

      env {
        name  = "POSTGRES_USER"
        value = var.postgres_admin_user
      }

      env {
        name        = "POSTGRES_PASSWORD"
        secret_name = "pg-password"
      }

      env {
        name  = "POSTGRES_SSLMODE"
        value = "require"
      }

      env {
        name  = "ENABLE_AZURE_PRICING_COLLECTOR"
        value = "false"
      }

      env {
        name  = "JOB_TYPE"
        value = "scheduled-spot"
      }

      env {
        name  = "PYTHONUNBUFFERED"
        value = "1"
      }

      env {
        name  = "LOG_LEVEL"
        value = var.log_level
      }

      env {
        name  = "ENABLE_AZURE_SPOT_COLLECTOR"
        value = tostring(var.enable_spot_collector)
      }

      env {
        name  = "AZURE_SPOT_MAX_ITEMS"
        value = var.max_spot_items
      }

      env {
        name  = "AZURE_CLIENT_ID"
        value = azurerm_user_assigned_identity.ingestion_jobs.client_id
      }
    }
  }

  secret {
    name  = "pg-password"
    value = var.postgres_admin_password
  }

  registry {
    server   = azurerm_container_registry.main.login_server
    identity = azurerm_user_assigned_identity.ingestion_jobs.id
  }

  tags = {
    Environment = var.environment
    Project     = "bdd-sku"
    JobType     = "scheduled"
  }
}

# ---------------------------------------------------------------------
# Container Apps Job – Spot Eviction Hourly (historization)
# ---------------------------------------------------------------------
resource "azurerm_container_app_job" "spot_eviction_hourly" {
  name                         = "${var.prefix}-spot-evict"
  location                     = azurerm_resource_group.main.location
  resource_group_name          = azurerm_resource_group.main.name
  container_app_environment_id = azurerm_container_app_environment.main.id

  identity {
    type         = "UserAssigned"
    identity_ids = [azurerm_user_assigned_identity.ingestion_jobs.id]
  }

  replica_timeout_in_seconds = 900
  replica_retry_limit        = 3

  schedule_trigger_config {
    cron_expression          = var.spot_eviction_cron
    parallelism              = 1
    replica_completion_count = 1
  }

  template {
    container {
      name   = "bdd-sku-spot-eviction"
      image  = "${azurerm_container_registry.main.login_server}/bdd-sku-ingestion:latest"
      cpu    = 0.5
      memory = "1Gi"

      env {
        name  = "POSTGRES_HOST"
        value = azurerm_postgresql_flexible_server.main.fqdn
      }

      env {
        name  = "POSTGRES_PORT"
        value = "5432"
      }

      env {
        name  = "POSTGRES_DB"
        value = var.postgres_db_name
      }

      env {
        name  = "POSTGRES_USER"
        value = var.postgres_admin_user
      }

      env {
        name        = "POSTGRES_PASSWORD"
        secret_name = "pg-password"
      }

      env {
        name  = "POSTGRES_SSLMODE"
        value = "require"
      }

      env {
        name  = "ENABLE_AZURE_PRICING_COLLECTOR"
        value = "false"
      }

      env {
        name  = "ENABLE_AZURE_SPOT_COLLECTOR"
        value = "true"
      }

      env {
        name  = "AZURE_SPOT_EVICTION_ONLY"
        value = "true"
      }

      env {
        name  = "AZURE_SPOT_MAX_ITEMS"
        value = var.max_spot_items
      }

      env {
        name  = "JOB_TYPE"
        value = "spot-eviction-hourly"
      }

      env {
        name  = "PYTHONUNBUFFERED"
        value = "1"
      }

      env {
        name  = "LOG_LEVEL"
        value = var.log_level
      }

      env {
        name  = "AZURE_CLIENT_ID"
        value = azurerm_user_assigned_identity.ingestion_jobs.client_id
      }
    }
  }

  secret {
    name  = "pg-password"
    value = var.postgres_admin_password
  }

  registry {
    server   = azurerm_container_registry.main.login_server
    identity = azurerm_user_assigned_identity.ingestion_jobs.id
  }

  tags = {
    Environment = var.environment
    Project     = "bdd-sku"
    JobType     = "spot-eviction-hourly"
  }
}

# ---------------------------------------------------------------------
# Container Apps Job – Manual (on-demand)
# ---------------------------------------------------------------------
resource "azurerm_container_app_job" "ingestion_manual" {
  name                         = "${var.prefix}-manual"
  location                     = azurerm_resource_group.main.location
  resource_group_name          = azurerm_resource_group.main.name
  container_app_environment_id = azurerm_container_app_environment.main.id

  identity {
    type         = "UserAssigned"
    identity_ids = [azurerm_user_assigned_identity.ingestion_jobs.id]
  }

  replica_timeout_in_seconds = 21600
  replica_retry_limit        = 0

  manual_trigger_config {
    parallelism              = 1
    replica_completion_count = 1
  }

  template {
    container {
      name   = "bdd-sku-ingestion"
      image  = "${azurerm_container_registry.main.login_server}/bdd-sku-ingestion:latest"
      cpu    = 1.0
      memory = "2Gi"

      env {
        name  = "POSTGRES_HOST"
        value = azurerm_postgresql_flexible_server.main.fqdn
      }

      env {
        name  = "POSTGRES_PORT"
        value = "5432"
      }

      env {
        name  = "POSTGRES_DB"
        value = var.postgres_db_name
      }

      env {
        name  = "POSTGRES_USER"
        value = var.postgres_admin_user
      }

      env {
        name        = "POSTGRES_PASSWORD"
        secret_name = "pg-password"
      }

      env {
        name  = "POSTGRES_SSLMODE"
        value = "require"
      }

      env {
        name  = "ENABLE_AZURE_PRICING_COLLECTOR"
        value = "true"
      }

      env {
        name  = "AZURE_PRICING_MAX_ITEMS"
        value = var.max_pricing_items
      }

      env {
        name  = "JOB_TYPE"
        value = "manual"
      }

      env {
        name  = "PYTHONUNBUFFERED"
        value = "1"
      }

      env {
        name  = "LOG_LEVEL"
        value = var.log_level
      }

      env {
        name  = "ENABLE_AZURE_SPOT_COLLECTOR"
        value = tostring(var.enable_spot_collector)
      }

      env {
        name  = "AZURE_SPOT_MAX_ITEMS"
        value = var.max_spot_items
      }

      env {
        name  = "AZURE_CLIENT_ID"
        value = azurerm_user_assigned_identity.ingestion_jobs.client_id
      }
    }
  }

  secret {
    name  = "pg-password"
    value = var.postgres_admin_password
  }

  registry {
    server   = azurerm_container_registry.main.login_server
    identity = azurerm_user_assigned_identity.ingestion_jobs.id
  }

  tags = {
    Environment = var.environment
    Project     = "bdd-sku"
    JobType     = "manual"
  }
}

# ---------------------------------------------------------------------
# Container Apps Job – SKU Mapper (daily at 04:00 UTC)
# ---------------------------------------------------------------------
resource "azurerm_container_app_job" "sku_mapper" {
  name                         = "sku-mapper-job"
  location                     = azurerm_resource_group.main.location
  resource_group_name          = azurerm_resource_group.main.name
  container_app_environment_id = azurerm_container_app_environment.main.id

  identity {
    type         = "UserAssigned"
    identity_ids = [azurerm_user_assigned_identity.ingestion_jobs.id]
  }

  replica_timeout_in_seconds = 3600
  replica_retry_limit        = 3

  schedule_trigger_config {
    cron_expression          = var.sku_mapper_cron
    parallelism              = 1
    replica_completion_count = 1
  }

  template {
    container {
      name   = "sku-mapper-job"
      image  = "${azurerm_container_registry.main.login_server}/sku-mapper-job:latest"
      cpu    = 0.5
      memory = "1Gi"

      env {
        name  = "PGHOST"
        value = azurerm_postgresql_flexible_server.main.fqdn
      }

      env {
        name  = "PGPORT"
        value = "5432"
      }

      env {
        name  = "PGDATABASE"
        value = var.postgres_db_name
      }

      env {
        name  = "PGUSER"
        value = var.postgres_admin_user
      }

      env {
        name        = "PGPASSWORD"
        secret_name = "pg-password"
      }

      env {
        name  = "PGSSLMODE"
        value = "require"
      }

      env {
        name  = "LOG_LEVEL"
        value = var.log_level
      }

      env {
        name  = "PYTHONUNBUFFERED"
        value = "1"
      }
    }
  }

  secret {
    name  = "pg-password"
    value = var.postgres_admin_password
  }

  registry {
    server   = azurerm_container_registry.main.login_server
    identity = azurerm_user_assigned_identity.ingestion_jobs.id
  }

  tags = {
    Environment = var.environment
    Project     = "bdd-sku"
    JobType     = "sku-mapper"
  }
}

# ---------------------------------------------------------------------
# Container Apps Job – Price Aggregator (scheduled, after sku-mapper)
# ---------------------------------------------------------------------
resource "azurerm_container_app_job" "price_aggregator" {
  name                         = "price-aggregator-job"
  location                     = azurerm_resource_group.main.location
  resource_group_name          = azurerm_resource_group.main.name
  container_app_environment_id = azurerm_container_app_environment.main.id

  identity {
    type         = "UserAssigned"
    identity_ids = [azurerm_user_assigned_identity.ingestion_jobs.id]
  }

  replica_timeout_in_seconds = 3600
  replica_retry_limit        = 3

  schedule_trigger_config {
    cron_expression          = var.price_aggregator_cron
    parallelism              = 1
    replica_completion_count = 1
  }

  template {
    container {
      name   = "price-aggregator-job"
      image  = "${azurerm_container_registry.main.login_server}/price-aggregator-job:latest"
      cpu    = 1
      memory = "2Gi"

      env {
        name  = "PGHOST"
        value = azurerm_postgresql_flexible_server.main.fqdn
      }

      env {
        name  = "PGPORT"
        value = "5432"
      }

      env {
        name  = "PGDATABASE"
        value = var.postgres_db_name
      }

      env {
        name  = "PGUSER"
        value = var.postgres_admin_user
      }

      env {
        name        = "PGPASSWORD"
        secret_name = "pg-password"
      }

      env {
        name  = "PGSSLMODE"
        value = "require"
      }

      env {
        name  = "LOG_LEVEL"
        value = var.log_level
      }

      env {
        name  = "PYTHONUNBUFFERED"
        value = "1"
      }
    }
  }

  secret {
    name  = "pg-password"
    value = var.postgres_admin_password
  }

  registry {
    server   = azurerm_container_registry.main.login_server
    identity = azurerm_user_assigned_identity.ingestion_jobs.id
  }

  tags = {
    Environment = var.environment
    Project     = "bdd-sku"
    JobType     = "price-aggregator"
  }
}

# ---------------------------------------------------------------------
# Container App – API (always-on, auto-scaling)
# ---------------------------------------------------------------------
resource "azurerm_container_app" "api" {
  name                         = "${var.prefix}-api"
  resource_group_name          = azurerm_resource_group.main.name
  container_app_environment_id = azurerm_container_app_environment.main.id
  revision_mode                = "Single"

  identity {
    type         = "UserAssigned"
    identity_ids = [azurerm_user_assigned_identity.ingestion_jobs.id]
  }

  registry {
    server   = azurerm_container_registry.main.login_server
    identity = azurerm_user_assigned_identity.ingestion_jobs.id
  }

  secret {
    name  = "pg-password"
    value = var.postgres_admin_password
  }

  ingress {
    external_enabled = true
    target_port      = var.api_port
    transport        = "auto"

    traffic_weight {
      latest_revision = true
      percentage      = 100
    }
  }

  template {
    min_replicas = var.api_min_replicas
    max_replicas = var.api_max_replicas

    http_scale_rule {
      name                = "http-concurrency"
      concurrent_requests = "50"
    }

    container {
      name   = "bdd-sku-api"
      image  = "${azurerm_container_registry.main.login_server}/bdd-sku-api:latest"
      cpu    = var.api_cpu
      memory = var.api_memory

      env {
        name  = "POSTGRES_HOST"
        value = azurerm_postgresql_flexible_server.main.fqdn
      }

      env {
        name  = "POSTGRES_PORT"
        value = "5432"
      }

      env {
        name  = "POSTGRES_DB"
        value = var.postgres_db_name
      }

      env {
        name  = "POSTGRES_USER"
        value = azurerm_user_assigned_identity.ingestion_jobs.name
      }

      env {
        name  = "POSTGRES_SSLMODE"
        value = "require"
      }

      env {
        name  = "POSTGRES_AUTH_METHOD"
        value = "msi"
      }

      env {
        name  = "AZURE_CLIENT_ID"
        value = azurerm_user_assigned_identity.ingestion_jobs.client_id
      }

      env {
        name  = "PYTHONUNBUFFERED"
        value = "1"
      }

      liveness_probe {
        transport = "HTTP"
        port      = var.api_port
        path      = "/health"

        initial_delay           = 5
        interval_seconds        = 30
        timeout                 = 5
        failure_count_threshold = 3
      }

      readiness_probe {
        transport = "HTTP"
        port      = var.api_port
        path      = "/health"

        interval_seconds        = 10
        timeout                 = 5
        failure_count_threshold = 3
      }
    }
  }

  tags = {
    Environment = var.environment
    Project     = "bdd-sku"
    Component   = "api"
  }
}
