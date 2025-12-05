#!/bin/bash
set -e

# Load variables
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/variables.sh"

# Data Lake specific variables
STORAGE_ACCOUNT_NAME="${STORAGE_ACCOUNT_NAME:-gaaborotrinity}"  # Must be globally unique, lowercase, no hyphens
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
    echo "Set it with: export SUBSCRIPTION_ID=\"your-subscription-id\""
    exit 1
fi

# Check if logged in to Azure
echo "Checking Azure login status..."
az account show > /dev/null 2>&1 || { echo "Please run 'az login' first"; exit 1; }

# Set subscription
echo "Setting subscription..."
az account set --subscription "$SUBSCRIPTION_ID"

# Create Resource Group if it doesn't exist
echo "Ensuring Resource Group exists..."
az group create \
    --name "$RESOURCE_GROUP" \
    --location "$LOCATION" \
    --output none 2>/dev/null || true

# Create Storage Account with Data Lake Gen2 (hierarchical namespace)
echo "Creating Storage Account with Data Lake Gen2..."
az storage account create \
    --name "$STORAGE_ACCOUNT_NAME" \
    --resource-group "$RESOURCE_GROUP" \
    --location "$LOCATION" \
    --sku Standard_LRS \
    --kind StorageV2 \
    --hns true \
    --enable-sftp false \
    --min-tls-version TLS1_2 \
    --allow-blob-public-access false

# Get storage account key
echo "Retrieving storage account key..."
STORAGE_KEY=$(az storage account keys list \
    --resource-group "$RESOURCE_GROUP" \
    --account-name "$STORAGE_ACCOUNT_NAME" \
    --query '[0].value' \
    --output tsv)

# Create containers (Bronze, Silver, Gold layers)
echo "Creating Bronze container (raw data)..."
az storage fs create \
    --name "$CONTAINER_BRONZE" \
    --account-name "$STORAGE_ACCOUNT_NAME" \
    --account-key "$STORAGE_KEY"

echo "Creating Silver container (cleaned data)..."
az storage fs create \
    --name "$CONTAINER_SILVER" \
    --account-name "$STORAGE_ACCOUNT_NAME" \
    --account-key "$STORAGE_KEY"

echo "Creating Gold container (business-ready data)..."
az storage fs create \
    --name "$CONTAINER_GOLD" \
    --account-name "$STORAGE_ACCOUNT_NAME" \
    --account-key "$STORAGE_KEY"

# Create directory structure in each container
echo "Creating directory structure..."

# Bronze: organized by source system and date
az storage fs directory create --name "erp" --file-system "$CONTAINER_BRONZE" --account-name "$STORAGE_ACCOUNT_NAME" --account-key "$STORAGE_KEY"
az storage fs directory create --name "erp/raw" --file-system "$CONTAINER_BRONZE" --account-name "$STORAGE_ACCOUNT_NAME" --account-key "$STORAGE_KEY"

# Silver: cleaned and validated
az storage fs directory create --name "erp" --file-system "$CONTAINER_SILVER" --account-name "$STORAGE_ACCOUNT_NAME" --account-key "$STORAGE_KEY"
az storage fs directory create --name "erp/cleaned" --file-system "$CONTAINER_SILVER" --account-name "$STORAGE_ACCOUNT_NAME" --account-key "$STORAGE_KEY"

# Gold: business aggregates
az storage fs directory create --name "analytics" --file-system "$CONTAINER_GOLD" --account-name "$STORAGE_ACCOUNT_NAME" --account-key "$STORAGE_KEY"
az storage fs directory create --name "serving" --file-system "$CONTAINER_GOLD" --account-name "$STORAGE_ACCOUNT_NAME" --account-key "$STORAGE_KEY"

# Get connection string
CONNECTION_STRING=$(az storage account show-connection-string \
    --resource-group "$RESOURCE_GROUP" \
    --name "$STORAGE_ACCOUNT_NAME" \
    --query connectionString \
    --output tsv)

echo ""
echo "=== Data Lake Created Successfully ==="
echo ""
echo "Storage Account: $STORAGE_ACCOUNT_NAME"
echo "Containers: bronze, silver, gold"
echo ""
echo "Connection Details:"
echo "-------------------"
echo "Account Name: $STORAGE_ACCOUNT_NAME"
echo "Account Key: $STORAGE_KEY"
echo ""
echo "Connection String:"
echo "$CONNECTION_STRING"
echo ""
echo "ABFS URLs (for Spark/Airflow):"
echo "  Bronze: abfss://${CONTAINER_BRONZE}@${STORAGE_ACCOUNT_NAME}.dfs.core.windows.net/"
echo "  Silver: abfss://${CONTAINER_SILVER}@${STORAGE_ACCOUNT_NAME}.dfs.core.windows.net/"
echo "  Gold:   abfss://${CONTAINER_GOLD}@${STORAGE_ACCOUNT_NAME}.dfs.core.windows.net/"
echo ""
echo "Save these credentials securely!"
