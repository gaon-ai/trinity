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
if [ -z "$SUBSCRIPTION_ID" ]; then
    print_error "SUBSCRIPTION_ID is not set!"
    echo "Run: export SUBSCRIPTION_ID=\"your-subscription-id\""
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
echo "Registering Azure providers (this may take a few minutes)..."

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
    # Try to create, if fails due to name conflict, add random suffix
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
# Configure Firewall (allow Azure services and your IP)
# =============================================================================
echo ""
echo "Configuring firewall rules..."

# Allow Azure services
az synapse workspace firewall-rule create \
    --name "AllowAllWindowsAzureIps" \
    --workspace-name "$SYNAPSE_WORKSPACE_NAME" \
    --resource-group "$RESOURCE_GROUP" \
    --start-ip-address 0.0.0.0 \
    --end-ip-address 0.0.0.0 \
    --only-show-errors 2>/dev/null || true

print_status "Allowed Azure services"

# Allow all IPs if configured (for development)
if [ "$ALLOW_ALL_IPS" = "true" ]; then
    az synapse workspace firewall-rule create \
        --name "AllowAll" \
        --workspace-name "$SYNAPSE_WORKSPACE_NAME" \
        --resource-group "$RESOURCE_GROUP" \
        --start-ip-address 0.0.0.0 \
        --end-ip-address 255.255.255.255 \
        --only-show-errors 2>/dev/null || true
    print_status "Allowed all IPs (development mode)"
else
    # Add current IP
    MY_IP=$(curl -s ifconfig.me)
    az synapse workspace firewall-rule create \
        --name "MyIP" \
        --workspace-name "$SYNAPSE_WORKSPACE_NAME" \
        --resource-group "$RESOURCE_GROUP" \
        --start-ip-address "$MY_IP" \
        --end-ip-address "$MY_IP" \
        --only-show-errors 2>/dev/null || true
    print_status "Allowed your IP: $MY_IP"
fi

# =============================================================================
# Grant Synapse access to Data Lake
# =============================================================================
echo ""
echo "Granting Synapse access to Data Lake..."

# Get Synapse managed identity
SYNAPSE_IDENTITY=$(az synapse workspace show \
    --name "$SYNAPSE_WORKSPACE_NAME" \
    --resource-group "$RESOURCE_GROUP" \
    --query "identity.principalId" -o tsv)

# Assign Storage Blob Data Contributor role
az role assignment create \
    --assignee "$SYNAPSE_IDENTITY" \
    --role "Storage Blob Data Contributor" \
    --scope "$STORAGE_RESOURCE_ID" \
    --only-show-errors 2>/dev/null || true

print_status "Granted Synapse managed identity access to Data Lake"

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
echo "Data Lake:"
echo "  Storage Account: $STORAGE_ACCOUNT_NAME"
echo "  Bronze: abfss://bronze@${STORAGE_ACCOUNT_NAME}.dfs.core.windows.net/"
echo "  Silver: abfss://silver@${STORAGE_ACCOUNT_NAME}.dfs.core.windows.net/"
echo "  Gold:   abfss://gold@${STORAGE_ACCOUNT_NAME}.dfs.core.windows.net/"
echo ""
echo "=============================================="
echo "  Sample Queries"
echo "=============================================="
echo ""
echo "-- Query Silver layer CSV files"
echo "SELECT TOP 100 *"
echo "FROM OPENROWSET("
echo "    BULK 'https://${STORAGE_ACCOUNT_NAME}.dfs.core.windows.net/silver/erp/sales/**/*.csv',"
echo "    FORMAT = 'CSV',"
echo "    HEADER_ROW = TRUE"
echo ") AS sales"
echo ""
echo "-- Query Gold layer JSON summary"
echo "SELECT TOP 10"
echo "    JSON_VALUE(doc, '\$.report_date') AS report_date,"
echo "    JSON_VALUE(doc, '\$.total_orders') AS total_orders,"
echo "    JSON_VALUE(doc, '\$.total_revenue') AS total_revenue"
echo "FROM OPENROWSET("
echo "    BULK 'https://${STORAGE_ACCOUNT_NAME}.dfs.core.windows.net/gold/serving/daily_summary/**/*.json',"
echo "    FORMAT = 'CSV',"
echo "    FIELDTERMINATOR = '0x0b',"
echo "    FIELDQUOTE = '0x0b'"
echo ") WITH (doc NVARCHAR(MAX)) AS rows"
echo ""
echo "=============================================="
echo ""
echo "Save these credentials securely!"
echo ""
