#!/bin/bash
set -e

# Load variables
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/variables.sh"

# Data Lake containers (Medallion Architecture)
CONTAINER_BRONZE="bronze"   # Raw data from source systems
CONTAINER_SILVER="silver"   # Cleaned, validated data
CONTAINER_GOLD="gold"       # Business-ready aggregated data

echo "=== Azure Data Lake Storage Gen2 Setup ==="
echo "Resource Group: $RESOURCE_GROUP"
echo "Location: $LOCATION"
echo "Storage Account: $STORAGE_ACCOUNT_NAME"
echo ""

# Check subscription ID is set
if [ -z "$SUBSCRIPTION_ID" ]; then
    echo "ERROR: SUBSCRIPTION_ID is not set."
    echo ""
    echo "Set it with:"
    echo "  export SUBSCRIPTION_ID=\"your-subscription-id\""
    exit 1
fi

# Check if logged in to Azure
echo "Checking Azure login status..."
if ! az account show > /dev/null 2>&1; then
    echo "Not logged in. Please run 'az login' first."
    exit 1
fi

# Set subscription
echo "Setting subscription..."
az account set --subscription "$SUBSCRIPTION_ID"

# Register Storage provider (learned from troubleshooting - this can take a minute)
echo "Registering Microsoft.Storage provider (may take a minute)..."
az provider register --namespace Microsoft.Storage --wait || true

# Create Resource Group if it doesn't exist
echo "Ensuring Resource Group exists..."
az group create \
    --name "$RESOURCE_GROUP" \
    --location "$LOCATION" \
    --output none

# Try to create storage account, add random suffix if name is taken
FINAL_STORAGE_NAME="$STORAGE_ACCOUNT_NAME"
echo "Creating Storage Account with Data Lake Gen2..."

if ! az storage account create \
    --name "$FINAL_STORAGE_NAME" \
    --resource-group "$RESOURCE_GROUP" \
    --location "$LOCATION" \
    --sku Standard_LRS \
    --kind StorageV2 \
    --hns true \
    --min-tls-version TLS1_2 \
    --allow-blob-public-access false \
    --output none 2>/dev/null; then

    # Name taken, try with random suffix
    RANDOM_SUFFIX=$(openssl rand -hex 3)
    FINAL_STORAGE_NAME="${STORAGE_ACCOUNT_NAME}${RANDOM_SUFFIX}"
    echo "Name taken, trying: $FINAL_STORAGE_NAME"

    az storage account create \
        --name "$FINAL_STORAGE_NAME" \
        --resource-group "$RESOURCE_GROUP" \
        --location "$LOCATION" \
        --sku Standard_LRS \
        --kind StorageV2 \
        --hns true \
        --min-tls-version TLS1_2 \
        --allow-blob-public-access false \
        --output none
fi

# Get storage account key
echo "Retrieving storage account key..."
STORAGE_KEY=$(az storage account keys list \
    --resource-group "$RESOURCE_GROUP" \
    --account-name "$FINAL_STORAGE_NAME" \
    --query '[0].value' \
    --output tsv)

# Create containers (Bronze, Silver, Gold layers)
echo "Creating Bronze container (raw data)..."
az storage fs create \
    --name "$CONTAINER_BRONZE" \
    --account-name "$FINAL_STORAGE_NAME" \
    --account-key "$STORAGE_KEY" \
    --output none

echo "Creating Silver container (cleaned data)..."
az storage fs create \
    --name "$CONTAINER_SILVER" \
    --account-name "$FINAL_STORAGE_NAME" \
    --account-key "$STORAGE_KEY" \
    --output none

echo "Creating Gold container (business-ready data)..."
az storage fs create \
    --name "$CONTAINER_GOLD" \
    --account-name "$FINAL_STORAGE_NAME" \
    --account-key "$STORAGE_KEY" \
    --output none

# Create directory structure in each container
echo "Creating directory structure..."

# Bronze: organized by source system
az storage fs directory create --name "erp" --file-system "$CONTAINER_BRONZE" \
    --account-name "$FINAL_STORAGE_NAME" --account-key "$STORAGE_KEY" --output none

# Silver: cleaned and validated
az storage fs directory create --name "erp" --file-system "$CONTAINER_SILVER" \
    --account-name "$FINAL_STORAGE_NAME" --account-key "$STORAGE_KEY" --output none

# Gold: business aggregates
az storage fs directory create --name "analytics" --file-system "$CONTAINER_GOLD" \
    --account-name "$FINAL_STORAGE_NAME" --account-key "$STORAGE_KEY" --output none
az storage fs directory create --name "serving" --file-system "$CONTAINER_GOLD" \
    --account-name "$FINAL_STORAGE_NAME" --account-key "$STORAGE_KEY" --output none

# Get connection string
CONNECTION_STRING=$(az storage account show-connection-string \
    --resource-group "$RESOURCE_GROUP" \
    --name "$FINAL_STORAGE_NAME" \
    --query connectionString \
    --output tsv)

echo ""
echo "=============================================="
echo "    Data Lake Created Successfully!          "
echo "=============================================="
echo ""
echo "Storage Account: $FINAL_STORAGE_NAME"
echo "Containers: bronze, silver, gold"
echo ""
echo "Connection Details:"
echo "-------------------"
echo "Account Name: $FINAL_STORAGE_NAME"
echo "Account Key:  $STORAGE_KEY"
echo ""
echo "ABFS URLs (for Spark/Airflow):"
echo "  Bronze: abfss://${CONTAINER_BRONZE}@${FINAL_STORAGE_NAME}.dfs.core.windows.net/"
echo "  Silver: abfss://${CONTAINER_SILVER}@${FINAL_STORAGE_NAME}.dfs.core.windows.net/"
echo "  Gold:   abfss://${CONTAINER_GOLD}@${FINAL_STORAGE_NAME}.dfs.core.windows.net/"
echo ""
echo "Connection String:"
echo "$CONNECTION_STRING"
echo ""
echo "IMPORTANT: Save these credentials securely!"
echo ""
