#!/bin/bash
# Create Azure SQL Database for serving layer (materialized views for Power BI)

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

# Validation
if ! validate_required_vars; then
    exit 1
fi

# Generate password if not set (still needed for initial SQL admin setup)
if [ -z "$SQL_ADMIN_PASSWORD" ]; then
    SQL_ADMIN_PASSWORD="$(openssl rand -base64 16 | tr -dc 'a-zA-Z0-9' | head -c 16)Aa1!"
fi

echo "=============================================="
echo "  Azure SQL Database Setup"
echo "=============================================="
echo ""
echo "Configuration:"
echo "  Subscription:    $SUBSCRIPTION_ID"
echo "  Resource Group:  $RESOURCE_GROUP"
echo "  Location:        $LOCATION"
echo "  SQL Server:      $SQL_SERVER_NAME"
echo "  Database:        $SQL_DATABASE_NAME"
echo "  SKU:             $SQL_SKU"
echo ""

# Set subscription
az account set --subscription "$SUBSCRIPTION_ID"
print_status "Using subscription: $SUBSCRIPTION_ID"

# =============================================================================
# Register SQL provider
# =============================================================================
echo ""
echo "Checking Azure SQL provider registration..."

STATUS=$(az provider show --namespace Microsoft.Sql --query "registrationState" -o tsv 2>/dev/null || echo "NotRegistered")
if [ "$STATUS" != "Registered" ]; then
    print_warning "Registering Microsoft.Sql provider..."
    az provider register --namespace Microsoft.Sql --wait
fi
print_status "Microsoft.Sql provider is registered"

# =============================================================================
# Create SQL Server
# =============================================================================
echo ""
echo "Creating SQL Server..."

# Check if server already exists
SERVER_EXISTS=$(az sql server show --name "$SQL_SERVER_NAME" --resource-group "$RESOURCE_GROUP" --query "name" -o tsv 2>/dev/null || echo "")

if [ -n "$SERVER_EXISTS" ]; then
    print_warning "SQL Server '$SQL_SERVER_NAME' already exists"
else
    # Try to create, if name is taken add random suffix
    if ! az sql server create \
        --name "$SQL_SERVER_NAME" \
        --resource-group "$RESOURCE_GROUP" \
        --location "$LOCATION" \
        --admin-user "$SQL_ADMIN_USER" \
        --admin-password "$SQL_ADMIN_PASSWORD" \
        --only-show-errors 2>/dev/null; then

        # Name might be taken, try with random suffix
        RANDOM_SUFFIX=$(openssl rand -hex 3)
        SQL_SERVER_NAME="${SQL_SERVER_NAME}${RANDOM_SUFFIX}"
        print_warning "Server name taken, trying: $SQL_SERVER_NAME"

        az sql server create \
            --name "$SQL_SERVER_NAME" \
            --resource-group "$RESOURCE_GROUP" \
            --location "$LOCATION" \
            --admin-user "$SQL_ADMIN_USER" \
            --admin-password "$SQL_ADMIN_PASSWORD"
    fi
    print_status "Created SQL Server: $SQL_SERVER_NAME"
fi

# =============================================================================
# Configure Firewall (Allow Azure services only - production mode)
# =============================================================================
echo ""
echo "Configuring firewall rules..."

# Allow Azure services (required for services within Azure to connect)
az sql server firewall-rule create \
    --server "$SQL_SERVER_NAME" \
    --resource-group "$RESOURCE_GROUP" \
    --name "AllowAzureServices" \
    --start-ip-address 0.0.0.0 \
    --end-ip-address 0.0.0.0 \
    --only-show-errors 2>/dev/null || true
print_status "Allowed Azure services"

# =============================================================================
# Private Endpoint Setup
# =============================================================================
echo ""
echo "Setting up Private Endpoint for Azure SQL..."

PE_NAME="pe-${SQL_SERVER_NAME}"
SQL_SERVER_ID=$(az sql server show \
    --name "$SQL_SERVER_NAME" \
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

    # Create Private Endpoint
    echo "Creating Private Endpoint: $PE_NAME"
    az network private-endpoint create \
        --name "$PE_NAME" \
        --resource-group "$RESOURCE_GROUP" \
        --vnet-name "$VNET_NAME" \
        --subnet "$SUBNET_NAME" \
        --private-connection-resource-id "$SQL_SERVER_ID" \
        --group-id sqlServer \
        --connection-name "${PE_NAME}-connection" \
        --output none 2>/dev/null || echo "Private Endpoint may already exist"

    # Create Private DNS Zone
    DNS_ZONE_NAME="privatelink.database.windows.net"
    echo "Setting up Private DNS Zone: $DNS_ZONE_NAME"

    az network private-dns zone create \
        --resource-group "$RESOURCE_GROUP" \
        --name "$DNS_ZONE_NAME" \
        --output none 2>/dev/null || true

    # Link DNS Zone to VNet
    az network private-dns link vnet create \
        --resource-group "$RESOURCE_GROUP" \
        --zone-name "$DNS_ZONE_NAME" \
        --name "${VNET_NAME}-sql-link" \
        --virtual-network "$VNET_NAME" \
        --registration-enabled false \
        --output none 2>/dev/null || true

    # Create DNS Zone Group
    az network private-endpoint dns-zone-group create \
        --resource-group "$RESOURCE_GROUP" \
        --endpoint-name "$PE_NAME" \
        --name "default" \
        --private-dns-zone "$DNS_ZONE_NAME" \
        --zone-name "sql" \
        --output none 2>/dev/null || true

    print_status "Private Endpoint configured!"
else
    print_warning "VNet/Subnet not found. Skipping Private Endpoint setup."
fi

# =============================================================================
# Create Database
# =============================================================================
echo ""
echo "Creating database..."

# Check if database already exists
DB_EXISTS=$(az sql db show --name "$SQL_DATABASE_NAME" --server "$SQL_SERVER_NAME" --resource-group "$RESOURCE_GROUP" --query "name" -o tsv 2>/dev/null || echo "")

if [ -n "$DB_EXISTS" ]; then
    print_warning "Database '$SQL_DATABASE_NAME' already exists"
else
    az sql db create \
        --name "$SQL_DATABASE_NAME" \
        --server "$SQL_SERVER_NAME" \
        --resource-group "$RESOURCE_GROUP" \
        --service-objective "$SQL_SKU" \
        --zone-redundant false
    print_status "Created database: $SQL_DATABASE_NAME"
fi

# =============================================================================
# Grant VM Managed Identity access to SQL
# =============================================================================
echo ""
echo "Setting up VM Managed Identity access..."

# Get VM Managed Identity if not provided
if [[ -z "$VM_IDENTITY_ID" ]]; then
    echo "Getting VM Managed Identity..."
    VM_IDENTITY_ID=$(az vm identity show \
        --resource-group "$RESOURCE_GROUP" \
        --name "$VM_NAME" \
        --query principalId -o tsv 2>/dev/null || echo "")
fi

if [[ -n "$VM_IDENTITY_ID" ]]; then
    print_status "VM Managed Identity found: $VM_IDENTITY_ID"
    echo ""
    echo "NOTE: To grant the VM Managed Identity access to Azure SQL,"
    echo "you need to run the following SQL commands as an Azure AD admin:"
    echo ""
    echo "-- Connect to the 'trinity' database, then run:"
    echo "CREATE USER [$VM_NAME] FROM EXTERNAL PROVIDER;"
    echo "ALTER ROLE db_datareader ADD MEMBER [$VM_NAME];"
    echo "ALTER ROLE db_datawriter ADD MEMBER [$VM_NAME];"
    echo "GRANT CREATE TABLE TO [$VM_NAME];"
    echo ""
else
    print_warning "Could not find VM Managed Identity."
fi

# =============================================================================
# Get connection details
# =============================================================================
SQL_SERVER_FQDN="${SQL_SERVER_NAME}.database.windows.net"

# =============================================================================
# Output Summary
# =============================================================================
echo ""
echo "=============================================="
echo "  Azure SQL Database Setup Complete!"
echo "=============================================="
echo ""
echo "Server Details:"
echo "  Server Name:     $SQL_SERVER_NAME"
echo "  Server FQDN:     $SQL_SERVER_FQDN"
echo "  Database:        $SQL_DATABASE_NAME"
echo "  Admin User:      $SQL_ADMIN_USER"
echo "  Admin Password:  $SQL_ADMIN_PASSWORD"
echo "  SKU:             $SQL_SKU"
echo ""
if [[ -n "$SUBNET_ID" ]]; then
    echo "Private Endpoint: $PE_NAME"
    echo "Private DNS Zone: privatelink.database.windows.net"
    echo ""
fi
echo "Access Method: VM Managed Identity (recommended)"
echo ""
echo "Power BI Connection:"
echo "  Server: $SQL_SERVER_FQDN"
echo "  Database: $SQL_DATABASE_NAME"
echo "  Authentication: Azure Active Directory"
echo ""
echo "=============================================="
echo ""
echo "Save these credentials securely!"
echo ""

# Export for use by other scripts
export SQL_SERVER="$SQL_SERVER_NAME"
export SQL_SERVER_FQDN="$SQL_SERVER_FQDN"
export SQL_DATABASE="$SQL_DATABASE_NAME"
export SQL_PASSWORD="$SQL_ADMIN_PASSWORD"
