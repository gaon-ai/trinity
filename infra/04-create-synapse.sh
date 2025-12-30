#!/bin/bash
# Create Azure Synapse Analytics Workspace (Serverless SQL)
# This enables ad-hoc SQL queries directly on Data Lake files

set -e

# Load configuration
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/variables.sh"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

print_status() { echo -e "${GREEN}[✓]${NC} $1"; }
print_warning() { echo -e "${YELLOW}[!]${NC} $1"; }
print_error() { echo -e "${RED}[✗]${NC} $1"; }

# =============================================================================
# Synapse Settings
# =============================================================================
SYNAPSE_WORKSPACE_NAME="${SYNAPSE_WORKSPACE_NAME:-trinitysynapse}"
SYNAPSE_SQL_ADMIN="sqladmin"
# Generate a random password if not set
SYNAPSE_SQL_PASSWORD="${SYNAPSE_SQL_PASSWORD:-$(openssl rand -base64 16 | tr -dc 'a-zA-Z0-9' | head -c 16)Aa1!}"

# Validation
if ! validate_required_vars; then
    exit 1
fi

echo "=============================================="
echo "  Azure Synapse Analytics Setup"
echo "=============================================="
echo ""
echo "Configuration:"
echo "  Subscription:    $SUBSCRIPTION_ID"
echo "  Resource Group:  $RESOURCE_GROUP"
echo "  Location:        $LOCATION"
echo "  Workspace Name:  $SYNAPSE_WORKSPACE_NAME"
echo "  Data Lake:       $STORAGE_ACCOUNT_NAME"
echo ""

# Set subscription
az account set --subscription "$SUBSCRIPTION_ID"
print_status "Using subscription: $SUBSCRIPTION_ID"

# =============================================================================
# Register required providers
# =============================================================================
echo ""
echo "Registering Azure providers..."

for provider in Microsoft.Synapse Microsoft.Storage Microsoft.Sql; do
    STATUS=$(az provider show --namespace $provider --query "registrationState" -o tsv 2>/dev/null || echo "NotRegistered")
    if [ "$STATUS" != "Registered" ]; then
        print_warning "Registering $provider..."
        az provider register --namespace $provider --wait
    fi
    print_status "$provider is registered"
done

# =============================================================================
# Check if storage account exists
# =============================================================================
echo ""
echo "Checking Data Lake storage account..."

STORAGE_EXISTS=$(az storage account show --name "$STORAGE_ACCOUNT_NAME" --resource-group "$RESOURCE_GROUP" --query "name" -o tsv 2>/dev/null || echo "")

if [ -z "$STORAGE_EXISTS" ]; then
    print_error "Storage account '$STORAGE_ACCOUNT_NAME' not found in resource group '$RESOURCE_GROUP'"
    echo "Please run 03-create-datalake.sh first, or set STORAGE_ACCOUNT_NAME to your existing storage account"
    exit 1
fi

print_status "Found storage account: $STORAGE_ACCOUNT_NAME"

# Get storage account resource ID
STORAGE_RESOURCE_ID=$(az storage account show --name "$STORAGE_ACCOUNT_NAME" --resource-group "$RESOURCE_GROUP" --query "id" -o tsv)

# =============================================================================
# Create Synapse Workspace
# =============================================================================
echo ""
echo "Creating Synapse workspace..."

# Check if workspace already exists
WORKSPACE_EXISTS=$(az synapse workspace show --name "$SYNAPSE_WORKSPACE_NAME" --resource-group "$RESOURCE_GROUP" --query "name" -o tsv 2>/dev/null || echo "")

if [ -n "$WORKSPACE_EXISTS" ]; then
    print_warning "Synapse workspace '$SYNAPSE_WORKSPACE_NAME' already exists"
else
    # Synapse workspace name must be globally unique
    if ! az synapse workspace create \
        --name "$SYNAPSE_WORKSPACE_NAME" \
        --resource-group "$RESOURCE_GROUP" \
        --location "$LOCATION" \
        --storage-account "$STORAGE_ACCOUNT_NAME" \
        --file-system "synapse" \
        --sql-admin-login-user "$SYNAPSE_SQL_ADMIN" \
        --sql-admin-login-password "$SYNAPSE_SQL_PASSWORD" \
        --only-show-errors 2>/dev/null; then

        # Name might be taken, try with random suffix
        RANDOM_SUFFIX=$(openssl rand -hex 3)
        SYNAPSE_WORKSPACE_NAME="${SYNAPSE_WORKSPACE_NAME}${RANDOM_SUFFIX}"
        print_warning "Workspace name taken, trying: $SYNAPSE_WORKSPACE_NAME"

        az synapse workspace create \
            --name "$SYNAPSE_WORKSPACE_NAME" \
            --resource-group "$RESOURCE_GROUP" \
            --location "$LOCATION" \
            --storage-account "$STORAGE_ACCOUNT_NAME" \
            --file-system "synapse" \
            --sql-admin-login-user "$SYNAPSE_SQL_ADMIN" \
            --sql-admin-login-password "$SYNAPSE_SQL_PASSWORD"
    fi

    print_status "Created Synapse workspace: $SYNAPSE_WORKSPACE_NAME"
fi

# =============================================================================
# Configure Firewall (Allow Azure services only - production mode)
# =============================================================================
echo ""
echo "Configuring firewall rules..."

# Allow Azure services (required for Synapse to access storage)
az synapse workspace firewall-rule create \
    --name "AllowAllWindowsAzureIps" \
    --workspace-name "$SYNAPSE_WORKSPACE_NAME" \
    --resource-group "$RESOURCE_GROUP" \
    --start-ip-address 0.0.0.0 \
    --end-ip-address 0.0.0.0 \
    --only-show-errors 2>/dev/null || true

print_status "Allowed Azure services"

# =============================================================================
# Private Endpoint Setup
# =============================================================================
echo ""
echo "Setting up Private Endpoint for Synapse..."

PE_NAME="pe-${SYNAPSE_WORKSPACE_NAME}"
SYNAPSE_ID=$(az synapse workspace show \
    --name "$SYNAPSE_WORKSPACE_NAME" \
    --resource-group "$RESOURCE_GROUP" \
    --query id -o tsv)

SUBNET_ID=$(az network vnet subnet show \
    --resource-group "$RESOURCE_GROUP" \
    --vnet-name "$VNET_NAME" \
    --name "$SUBNET_NAME" \
    --query id -o tsv 2>/dev/null || echo "")

if [[ -n "$SUBNET_ID" ]]; then
    # Disable private endpoint network policies on subnet (may already be done)
    az network vnet subnet update \
        --resource-group "$RESOURCE_GROUP" \
        --vnet-name "$VNET_NAME" \
        --name "$SUBNET_NAME" \
        --disable-private-endpoint-network-policies true \
        --output none 2>/dev/null || true

    # Create Private Endpoint for Serverless SQL
    echo "Creating Private Endpoint: $PE_NAME"
    az network private-endpoint create \
        --name "$PE_NAME" \
        --resource-group "$RESOURCE_GROUP" \
        --vnet-name "$VNET_NAME" \
        --subnet "$SUBNET_NAME" \
        --private-connection-resource-id "$SYNAPSE_ID" \
        --group-id SqlOnDemand \
        --connection-name "${PE_NAME}-connection" \
        --output none 2>/dev/null || echo "Private Endpoint may already exist"

    # Create Private DNS Zone
    DNS_ZONE_NAME="privatelink.sql.azuresynapse.net"
    echo "Setting up Private DNS Zone: $DNS_ZONE_NAME"

    az network private-dns zone create \
        --resource-group "$RESOURCE_GROUP" \
        --name "$DNS_ZONE_NAME" \
        --output none 2>/dev/null || true

    # Link DNS Zone to VNet
    az network private-dns link vnet create \
        --resource-group "$RESOURCE_GROUP" \
        --zone-name "$DNS_ZONE_NAME" \
        --name "${VNET_NAME}-synapse-link" \
        --virtual-network "$VNET_NAME" \
        --registration-enabled false \
        --output none 2>/dev/null || true

    # Create DNS Zone Group
    az network private-endpoint dns-zone-group create \
        --resource-group "$RESOURCE_GROUP" \
        --endpoint-name "$PE_NAME" \
        --name "default" \
        --private-dns-zone "$DNS_ZONE_NAME" \
        --zone-name "synapse" \
        --output none 2>/dev/null || true

    print_status "Private Endpoint configured!"
else
    print_warning "VNet/Subnet not found. Skipping Private Endpoint setup."
fi

# =============================================================================
# Grant Synapse access to Data Lake (Managed Identity)
# =============================================================================
echo ""
echo "Granting Synapse access to Data Lake..."

# Get Synapse managed identity
SYNAPSE_IDENTITY=$(az synapse workspace show \
    --name "$SYNAPSE_WORKSPACE_NAME" \
    --resource-group "$RESOURCE_GROUP" \
    --query "identity.principalId" -o tsv)

# Assign Storage Blob Data Reader role (read-only for queries)
az role assignment create \
    --assignee "$SYNAPSE_IDENTITY" \
    --role "Storage Blob Data Reader" \
    --scope "$STORAGE_RESOURCE_ID" \
    --only-show-errors 2>/dev/null || true

print_status "Granted Synapse managed identity read access to Data Lake"

# =============================================================================
# Get connection details
# =============================================================================
echo ""
echo "Getting connection details..."

# Get serverless SQL endpoint
SQL_ENDPOINT=$(az synapse workspace show \
    --name "$SYNAPSE_WORKSPACE_NAME" \
    --resource-group "$RESOURCE_GROUP" \
    --query "connectivityEndpoints.sqlOnDemand" -o tsv)

# Get Synapse Studio URL
STUDIO_URL="https://web.azuresynapse.net?workspace=/subscriptions/$SUBSCRIPTION_ID/resourceGroups/$RESOURCE_GROUP/providers/Microsoft.Synapse/workspaces/$SYNAPSE_WORKSPACE_NAME"

# =============================================================================
# Output Summary
# =============================================================================
echo ""
echo "=============================================="
echo "  Synapse Analytics Setup Complete!"
echo "=============================================="
echo ""
echo "Workspace Details:"
echo "  Name:           $SYNAPSE_WORKSPACE_NAME"
echo "  Resource Group: $RESOURCE_GROUP"
echo "  Location:       $LOCATION"
echo ""
echo "Connection Details:"
echo "  SQL Endpoint:   $SQL_ENDPOINT"
echo "  SQL Admin:      $SYNAPSE_SQL_ADMIN"
echo "  SQL Password:   $SYNAPSE_SQL_PASSWORD"
echo ""
echo "Synapse Studio (Web UI):"
echo "  $STUDIO_URL"
echo ""
echo "Data Lake Access: Via Managed Identity (no storage key needed)"
echo ""
if [[ -n "$SUBNET_ID" ]]; then
    echo "Private Endpoint: $PE_NAME"
    echo "Private DNS Zone: privatelink.sql.azuresynapse.net"
fi
echo ""
echo "=============================================="
echo ""
echo "Save these credentials securely!"
echo ""

# Export for use by other scripts
export SYNAPSE_WORKSPACE="$SYNAPSE_WORKSPACE_NAME"
export SYNAPSE_SQL_ENDPOINT="$SQL_ENDPOINT"
export SYNAPSE_ADMIN_PASSWORD="$SYNAPSE_SQL_PASSWORD"
