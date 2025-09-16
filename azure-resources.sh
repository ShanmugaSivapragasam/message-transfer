#!/usr/bin/env bash
set -euo pipefail

# ==== Prereqs ====
# brew install azure-cli
# az login

# ====== CONFIG ======
SUBSCRIPTION="your_subscription_id"
LOCATION="eastus"
RG="rg-message-transfer-poc"

BLUE_NS="shan-bg-blue"
GREEN_NS="shan-bg-green"

BLUE_QUEUE="orders-scheduled-blue"
GREEN_QUEUE="orders-scheduled-green"
ERROR_QUEUE="poc-dead-letter"     # used by the POC app for transfer failures

# Optional Redis (set to empty to skip)
REDIS_NAME="mtpoc-redis"

ensure_provider() {
  local NS="$1"
  local state
  state=$(az provider show -n "$NS" --query 'registrationState' -o tsv 2>/dev/null || echo "NotRegistered")
  if [ "$state" != "Registered" ]; then
    echo ">> Registering provider: $NS"
    az provider register -n "$NS" >/dev/null
    printf "   waiting for %s to be Registered" "$NS"
    for i in {1..60}; do
      state=$(az provider show -n "$NS" --query 'registrationState' -o tsv 2>/dev/null || echo "NotRegistered")
      [ "$state" = "Registered" ] && { echo " ✓"; break; }
      printf "."
      sleep 5
    done
    echo
  fi
}

# ====== AZURE CONTEXT ======
az account set -s "$SUBSCRIPTION"

# ====== RESOURCE GROUP ======
echo ">> Creating resource group: $RG ($LOCATION)"
az group create -n "$RG" -l "$LOCATION" -o table

# ====== SERVICE BUS NAMESPACES (Standard for scheduling) ======
echo ">> Creating Service Bus namespaces (Standard tier)"
az servicebus namespace create -g "$RG" -n "$BLUE_NS"  -l "$LOCATION" --sku Standard -o table
az servicebus namespace create -g "$RG" -n "$GREEN_NS" -l "$LOCATION" --sku Standard -o table

# ====== QUEUES ======
# Enable DL on expiration; tweak as you like (lock-duration etc.)
echo ">> Creating queues"
az servicebus queue create -g "$RG" --namespace-name "$BLUE_NS"  -n "$BLUE_QUEUE"  \
  --enable-dead-lettering-on-message-expiration true -o table
az servicebus queue create -g "$RG" --namespace-name "$GREEN_NS" -n "$GREEN_QUEUE" \
  --enable-dead-lettering-on-message-expiration true -o table

# POC "dead-letter" helper queue lives in the DEST namespace (matches the sample app)
az servicebus queue create -g "$RG" --namespace-name "$GREEN_NS" -n "$ERROR_QUEUE" \
  --enable-dead-lettering-on-message-expiration true -o table

# ====== SAS POLICIES (namespace-level) ======
# For the POC, we create a single policy with Manage+Send+Listen so you can schedule AND cancel.
# In prod, split rights and prefer Managed Identity/RBAC instead of SAS if possible.
echo ">> Creating namespace SAS policies"
az servicebus namespace authorization-rule create \
  -g "$RG" --namespace-name "$BLUE_NS"  --authorization-rule-name "app-manage" \
  --rights Manage Send Listen -o table

az servicebus namespace authorization-rule create \
  -g "$RG" --namespace-name "$GREEN_NS" --authorization-rule-name "app-manage" \
  --rights Manage Send Listen -o table

echo ">> Fetching connection strings"
BLUE_CS=$(az servicebus namespace authorization-rule keys list \
  -g "$RG" --namespace-name "$BLUE_NS"  --name "app-manage" \
  --query primaryConnectionString -o tsv)

GREEN_CS=$(az servicebus namespace authorization-rule keys list \
  -g "$RG" --namespace-name "$GREEN_NS" --name "app-manage" \
  --query primaryConnectionString -o tsv)

echo "BLUE connection string:  ${BLUE_CS:0:60}..."
echo "GREEN connection string: ${GREEN_CS:0:60}..."

# before az redis create:
ensure_provider Microsoft.Cache

# ====== OPTIONAL: AZURE CACHE FOR REDIS (Basic c0) ======
if [[ -n "${REDIS_NAME:-}" ]]; then
  echo ">> Creating Azure Cache for Redis (Basic c0) named $REDIS_NAME"
  az redis create -g "$RG" -n "$REDIS_NAME" -l "$LOCATION" --sku Basic --vm-size c0 -o table

  REDIS_HOST=$(az redis show -g "$RG" -n "$REDIS_NAME" --query hostName -o tsv)
  REDIS_KEY=$(az redis list-keys -g "$RG" -n "$REDIS_NAME" --query primaryKey -o tsv)
  echo "Redis host: $REDIS_HOST"
  echo "Redis key : ${REDIS_KEY:0:6}..."

  # For Spring Boot (Lettuce) using TLS port 6380:
  echo
  echo ">> To use Azure Redis from Spring Boot, add to application.properties:"
  echo "spring.data.redis.host=${REDIS_HOST}"
  echo "spring.data.redis.port=6380"
  echo "spring.data.redis.password=${REDIS_KEY}"
  echo "spring.data.redis.ssl.enabled=true"
fi

# ====== HELPERS: write values into your Spring app's application.properties ======
# Run from the repo root (where src/main/resources/application.properties exists).
APP_PROPS="src/main/resources/application.properties"
if [[ -f "$APP_PROPS" ]]; then
  echo ">> Updating $APP_PROPS with connection strings and queue names"
  # macOS sed needs the empty '' after -i
  sed -i '' "s#^azure.servicebus.source.connection-string=.*#azure.servicebus.source.connection-string=${BLUE_CS//#/\\#}#g" "$APP_PROPS"
  sed -i '' "s#^azure.servicebus.dest.connection-string=.*#azure.servicebus.dest.connection-string=${GREEN_CS//#/\\#}#g" "$APP_PROPS"
  sed -i '' "s#^azure.servicebus.source.queue=.*#azure.servicebus.source.queue=${BLUE_QUEUE}#g" "$APP_PROPS"
  sed -i '' "s#^azure.servicebus.dest.queue=.*#azure.servicebus.dest.queue=${GREEN_QUEUE}#g" "$APP_PROPS"
  # Error queue is already 'poc-dead-letter' by default, but ensure it:
  if grep -q "^azure.servicebus.error.queue=" "$APP_PROPS"; then
    sed -i '' "s#^azure.servicebus.error.queue=.*#azure.servicebus.error.queue=${ERROR_QUEUE}#g" "$APP_PROPS"
  else
    printf "\nazure.servicebus.error.queue=%s\n" "$ERROR_QUEUE" >> "$APP_PROPS"
  fi
else
  echo ">> Skipped updating application.properties (file not found at $APP_PROPS)"
fi

# ====== VERIFY ======
echo ">> Verifying resources"
az servicebus namespace show -g "$RG" -n "$BLUE_NS"  -o table
az servicebus namespace show -g "$RG" -n "$GREEN_NS" -o table
az servicebus queue list      -g "$RG" --namespace-name "$BLUE_NS"  -o table
az servicebus queue list      -g "$RG" --namespace-name "$GREEN_NS" -o table

echo
echo "All set. Plug the two connection strings into your app and run:"
echo "  ./gradlew bootRun"
 