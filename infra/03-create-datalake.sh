#!/bin/bash
set -e

# Load variables
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/variables.sh"

# Data Lake containers (Medallion Architecture)
CONTAINER_BRONZE="bronze"   # Raw data from source systems
CONTAINER_SILVER="silver"   # Cleaned, validated data
CONTAINER_GOLD="gold"       # Business-ready aggregated data
CONTAINER_SYNAPSE="synapse" # Synapse workspace storage

echo "=== Azure Data Lake Storage Gen2 Setup ==="
echo "Resource Group: $RESOURCE_GROUP"
echo "Location: $LOCATION"
echo "Storage Account: $STORAGE_ACCOUNT_NAME"
echo ""

# Validate required variables
if ! validate_required_vars; then
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

# Register Storage provider
echo "Registering Microsoft.Storage provider..."
az provider register --namespace Microsoft.Storage --wait || true

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

# Get storage account key (still needed for container creation)
echo "Retrieving storage account key..."
STORAGE_KEY=$(az storage account keys list \
    --resource-group "$RESOURCE_GROUP" \
    --account-name "$FINAL_STORAGE_NAME" \
    --query '[0].value' \
    --output tsv)

# Create containers (Bronze, Silver, Gold, Synapse layers)
echo "Creating Bronze container (raw data)..."
az storage fs create \
    --name "$CONTAINER_BRONZE" \
    --account-name "$FINAL_STORAGE_NAME" \
    --account-key "$STORAGE_KEY" \
    --output none 2>/dev/null || true

echo "Creating Silver container (cleaned data)..."
az storage fs create \
    --name "$CONTAINER_SILVER" \
    --account-name "$FINAL_STORAGE_NAME" \
    --account-key "$STORAGE_KEY" \
    --output none 2>/dev/null || true

echo "Creating Gold container (business-ready data)..."
az storage fs create \
    --name "$CONTAINER_GOLD" \
    --account-name "$FINAL_STORAGE_NAME" \
    --account-key "$STORAGE_KEY" \
    --output none 2>/dev/null || true

echo "Creating Synapse container..."
az storage fs create \
    --name "$CONTAINER_SYNAPSE" \
    --account-name "$FINAL_STORAGE_NAME" \
    --account-key "$STORAGE_KEY" \
    --output none 2>/dev/null || true

# Create directory structure in each container
echo "Creating directory structure..."

# Bronze: organized by source system
az storage fs directory create --name "erp" --file-system "$CONTAINER_BRONZE" \
    --account-name "$FINAL_STORAGE_NAME" --account-key "$STORAGE_KEY" --output none 2>/dev/null || true

# Silver: cleaned and validated
az storage fs directory create --name "erp" --file-system "$CONTAINER_SILVER" \
    --account-name "$FINAL_STORAGE_NAME" --account-key "$STORAGE_KEY" --output none 2>/dev/null || true

# Gold: business aggregates
az storage fs directory create --name "analytics" --file-system "$CONTAINER_GOLD" \
    --account-name "$FINAL_STORAGE_NAME" --account-key "$STORAGE_KEY" --output none 2>/dev/null || true
az storage fs directory create --name "serving" --file-system "$CONTAINER_GOLD" \
    --account-name "$FINAL_STORAGE_NAME" --account-key "$STORAGE_KEY" --output none 2>/dev/null || true

# =============================================================================
# Private Endpoint Setup
# =============================================================================
echo ""
echo "Setting up Private Endpoint for Data Lake..."

PE_NAME="pe-${FINAL_STORAGE_NAME}"
STORAGE_ID=$(az storage account show \
    --name "$FINAL_STORAGE_NAME" \
    --resource-group "$RESOURCE_GROUP" \
    --query id -o tsv)

SUBNET_ID=$(az network vnet subnet show \
    --resource-group "$RESOURCE_GROUP" \
    --vnet-name "$VNET_NAME" \
    --name "$SUBNET_NAME" \
    --query id -o tsv 2>/dev/null || echo "")

if [[ -n "$SUBNET_ID" ]]; then
    # Disable private endpoint network policies on subnet
    echo "Configuring subnet for Private Endpoints..."
    az network vnet subnet update \
        --resource-group "$RESOURCE_GROUP" \
        --vnet-name "$VNET_NAME" \
        --name "$SUBNET_NAME" \
        --disable-private-endpoint-network-policies true \
        --output none 2>/dev/null || true

    # Create Private Endpoint
    echo "Creating Private Endpoint: $PE_NAME"
    az network private-endpoint create \
        --name "$PE_NAME" \
        --resource-group "$RESOURCE_GROUP" \
        --vnet-name "$VNET_NAME" \
        --subnet "$SUBNET_NAME" \
        --private-connection-resource-id "$STORAGE_ID" \
        --group-id dfs \
        --connection-name "${PE_NAME}-connection" \
        --output none 2>/dev/null || echo "Private Endpoint may already exist"

    # Create Private DNS Zone
    DNS_ZONE_NAME="privatelink.dfs.core.windows.net"
    echo "Setting up Private DNS Zone: $DNS_ZONE_NAME"

    az network private-dns zone create \
        --resource-group "$RESOURCE_GROUP" \
        --name "$DNS_ZONE_NAME" \
        --output none 2>/dev/null || true

    # Link DNS Zone to VNet
    az network private-dns link vnet create \
        --resource-group "$RESOURCE_GROUP" \
        --zone-name "$DNS_ZONE_NAME" \
        --name "${VNET_NAME}-datalake-link" \
        --virtual-network "$VNET_NAME" \
        --registration-enabled false \
        --output none 2>/dev/null || true

    # Create DNS Zone Group for automatic DNS registration
    az network private-endpoint dns-zone-group create \
        --resource-group "$RESOURCE_GROUP" \
        --endpoint-name "$PE_NAME" \
        --name "default" \
        --private-dns-zone "$DNS_ZONE_NAME" \
        --zone-name "dfs" \
        --output none 2>/dev/null || true

    echo "Private Endpoint configured!"
else
    echo "WARNING: VNet/Subnet not found. Skipping Private Endpoint setup."
    echo "You may need to create it manually or check VNET_NAME and SUBNET_NAME variables."
fi

# =============================================================================
# Role Assignment for VM Managed Identity
# =============================================================================
echo ""
echo "Setting up role assignments..."

# Get VM Managed Identity if not provided
if [[ -z "$VM_IDENTITY_ID" ]]; then
    echo "Getting VM Managed Identity..."
    VM_IDENTITY_ID=$(az vm identity show \
        --resource-group "$RESOURCE_GROUP" \
        --name "$VM_NAME" \
        --query principalId -o tsv 2>/dev/null || echo "")
fi

if [[ -n "$VM_IDENTITY_ID" ]]; then
    echo "Assigning Storage Blob Data Contributor role to VM..."
    az role assignment create \
        --assignee "$VM_IDENTITY_ID" \
        --role "Storage Blob Data Contributor" \
        --scope "$STORAGE_ID" \
        --output none 2>/dev/null || echo "Role may already be assigned"
    echo "Role assigned: Storage Blob Data Contributor -> VM Managed Identity"
else
    echo "WARNING: Could not find VM Managed Identity."
    echo "You may need to assign the role manually:"
    echo "  az role assignment create --assignee <VM_IDENTITY_ID> --role 'Storage Blob Data Contributor' --scope $STORAGE_ID"
fi

# =============================================================================
# Output Summary
# =============================================================================
echo ""
echo "=============================================="
echo "    Data Lake Created Successfully!          "
echo "=============================================="
echo ""
echo "Storage Account: $FINAL_STORAGE_NAME"
echo "Containers: bronze, silver, gold, synapse"
echo ""
echo "ABFS URLs (for Airflow with Managed Identity):"
echo "  Bronze: abfss://${CONTAINER_BRONZE}@${FINAL_STORAGE_NAME}.dfs.core.windows.net/"
echo "  Silver: abfss://${CONTAINER_SILVER}@${FINAL_STORAGE_NAME}.dfs.core.windows.net/"
echo "  Gold:   abfss://${CONTAINER_GOLD}@${FINAL_STORAGE_NAME}.dfs.core.windows.net/"
echo ""
echo "Access Method: VM Managed Identity (no storage key needed)"
echo ""
if [[ -n "$SUBNET_ID" ]]; then
    echo "Private Endpoint: $PE_NAME"
    echo "Private DNS Zone: $DNS_ZONE_NAME"
fi
echo ""

# Export for use by other scripts
export DATALAKE_STORAGE_ACCOUNT="$FINAL_STORAGE_NAME"
export DATALAKE_STORAGE_ID="$STORAGE_ID"
