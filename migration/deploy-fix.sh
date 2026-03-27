#!/bin/bash
# Deploy commands for price-aggregator-job fix
# ==============================================

# 1. Create the covering index on PostgreSQL
PGPASSWORD='Q!PM6CP39z7QgF!@' psql -h az-scout-pg.postgres.database.azure.com -U azscout -d azscout -c \
  "CREATE INDEX IF NOT EXISTS idx_retail_vm_pricing_type_price ON retail_prices_vm (pricing_type, unit_price) WHERE unit_price > 0;"

# 2. Build & push the new Docker image
cd /mnt/d/Github/az-scout-bdd-ingestion/price-aggregator-job
docker build -t azscoutacr.azurecr.io/price-aggregator-job:latest .
az acr login --name azscoutacr
docker push azscoutacr.azurecr.io/price-aggregator-job:latest

# 3. Apply Terraform changes (timeout 30m->60m, CPU 0.5->1, RAM 1Gi->2Gi)
cd /mnt/d/Github/az-scout-bdd-ingestion/infra
terraform plan -target=azurerm_container_app_job.price_aggregator
terraform apply -target=azurerm_container_app_job.price_aggregator
