#!/bin/bash
# Unified Deployment Script for Trinity Data Lakehouse Platform
#
# Prerequisites:
#   - Azure CLI installed and logged in (az login)
#   - Infrastructure team has provided: VM, VNet, Subnet
#   - Environment variables set (see below)
#
# Required Environment Variables (from infra team):
#   export SUBSCRIPTION_ID="your-subscription-id"
#   export RESOURCE_GROUP="your-resource-group"
#   export VM_NAME="airflow-vm"
#   export VNET_NAME="airflow-vnet"
#   export SUBNET_NAME="airflow-subnet"

set -e

# Load configuration
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/variables.sh"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

print_status() { echo -e "${GREEN}[✓]${NC} $1"; }
print_warning() { echo -e "${YELLOW}[!]${NC} $1"; }
print_error() { echo -e "${RED}[✗]${NC} $1"; }
print_header() { echo -e "\n${BLUE}=== $1 ===${NC}\n"; }

# =============================================================================
# Pre-flight Checks
# =============================================================================
preflight_checks() {
    print_header "Pre-flight Checks"

    # Check Azure CLI
    if ! command -v az &> /dev/null; then
        print_error "Azure CLI not found. Please install: https://docs.microsoft.com/cli/azure/install-azure-cli"
        exit 1
    fi
    print_status "Azure CLI installed"

    # Check Azure login
    if ! az account show &> /dev/null; then
        print_error "Not logged in to Azure. Please run: az login"
        exit 1
    fi
    print_status "Logged in to Azure"

    # Validate required variables
    if ! validate_required_vars; then
        exit 1
    fi
    print_status "Required variables set"

    # Set subscription
    az account set --subscription "$SUBSCRIPTION_ID"
    print_status "Using subscription: $SUBSCRIPTION_ID"

    # Check VM exists
    if ! az vm show --name "$VM_NAME" --resource-group "$RESOURCE_GROUP" &> /dev/null; then
        print_error "VM '$VM_NAME' not found in resource group '$RESOURCE_GROUP'"
        echo "Please ensure the infrastructure team has created the VM."
        exit 1
    fi
    print_status "VM '$VM_NAME' exists"

    # Check VM has Managed Identity
    VM_IDENTITY=$(az vm identity show --name "$VM_NAME" --resource-group "$RESOURCE_GROUP" --query "principalId" -o tsv 2>/dev/null || echo "")
    if [ -z "$VM_IDENTITY" ]; then
        print_error "VM '$VM_NAME' does not have a Managed Identity enabled"
        echo "Please ask the infrastructure team to enable System Assigned Managed Identity on the VM."
        exit 1
    fi
    export VM_IDENTITY_ID="$VM_IDENTITY"
    print_status "VM Managed Identity: $VM_IDENTITY_ID"

    # Check VNet exists
    if ! az network vnet show --name "$VNET_NAME" --resource-group "$RESOURCE_GROUP" &> /dev/null; then
        print_error "VNet '$VNET_NAME' not found in resource group '$RESOURCE_GROUP'"
        exit 1
    fi
    print_status "VNet '$VNET_NAME' exists"

    # Check Subnet exists
    if ! az network vnet subnet show --vnet-name "$VNET_NAME" --name "$SUBNET_NAME" --resource-group "$RESOURCE_GROUP" &> /dev/null; then
        print_error "Subnet '$SUBNET_NAME' not found in VNet '$VNET_NAME'"
        exit 1
    fi
    print_status "Subnet '$SUBNET_NAME' exists"

    echo ""
    print_status "All pre-flight checks passed!"
}

# =============================================================================
# Create Data Lake
# =============================================================================
create_datalake() {
    print_header "Creating Data Lake (ADLS Gen2)"

    if [ -f "$SCRIPT_DIR/03-create-datalake.sh" ]; then
        bash "$SCRIPT_DIR/03-create-datalake.sh"
    else
        print_error "03-create-datalake.sh not found"
        exit 1
    fi
}

# =============================================================================
# Create Synapse
# =============================================================================
create_synapse() {
    print_header "Creating Synapse Analytics"

    if [ -f "$SCRIPT_DIR/04-create-synapse.sh" ]; then
        bash "$SCRIPT_DIR/04-create-synapse.sh"
    else
        print_error "04-create-synapse.sh not found"
        exit 1
    fi
}

# =============================================================================
# Create Azure SQL
# =============================================================================
create_sql() {
    print_header "Creating Azure SQL Database"

    if [ -f "$SCRIPT_DIR/05-create-azure-sql.sh" ]; then
        bash "$SCRIPT_DIR/05-create-azure-sql.sh"
    else
        print_error "05-create-azure-sql.sh not found"
        exit 1
    fi
}

# =============================================================================
# Generate Configuration File
# =============================================================================
generate_config() {
    print_header "Generating Configuration"

    CONFIG_FILE="$SCRIPT_DIR/../.env.generated"

    # Get actual values (scripts may have added suffixes)
    ACTUAL_STORAGE=$(az storage account list --resource-group "$RESOURCE_GROUP" --query "[?starts_with(name, '${STORAGE_ACCOUNT_NAME}')].name" -o tsv | head -1)
    ACTUAL_SYNAPSE=$(az synapse workspace list --resource-group "$RESOURCE_GROUP" --query "[?starts_with(name, '${SYNAPSE_WORKSPACE_NAME}')].name" -o tsv | head -1)
    ACTUAL_SQL=$(az sql server list --resource-group "$RESOURCE_GROUP" --query "[?starts_with(name, '${SQL_SERVER_NAME}')].name" -o tsv | head -1)

    # Get endpoints
    SYNAPSE_ENDPOINT=$(az synapse workspace show --name "$ACTUAL_SYNAPSE" --resource-group "$RESOURCE_GROUP" --query "connectivityEndpoints.sqlOnDemand" -o tsv 2>/dev/null || echo "")

    cat > "$CONFIG_FILE" << EOF
# Trinity Data Lakehouse Configuration
# Generated on $(date)
#
# NOTE: This platform uses Managed Identity for authentication.
# No storage keys or SQL passwords are stored here.

# Azure Settings
SUBSCRIPTION_ID=$SUBSCRIPTION_ID
RESOURCE_GROUP=$RESOURCE_GROUP
LOCATION=$LOCATION

# VM (created by infra team)
VM_NAME=$VM_NAME
VM_IDENTITY_ID=$VM_IDENTITY_ID

# Data Lake (ADLS Gen2)
STORAGE_ACCOUNT_NAME=$ACTUAL_STORAGE
STORAGE_ACCOUNT_URL=https://${ACTUAL_STORAGE}.dfs.core.windows.net

# Synapse Analytics
SYNAPSE_WORKSPACE=$ACTUAL_SYNAPSE
SYNAPSE_SQL_ENDPOINT=$SYNAPSE_ENDPOINT

# Azure SQL Database
SQL_SERVER_NAME=$ACTUAL_SQL
SQL_SERVER_FQDN=${ACTUAL_SQL}.database.windows.net
SQL_DATABASE_NAME=$SQL_DATABASE_NAME

# Airflow Settings
AIRFLOW_UID=50000
AIRFLOW__CORE__EXECUTOR=LocalExecutor
AIRFLOW__CORE__LOAD_EXAMPLES=false

# Authentication: Managed Identity
# The VM's Managed Identity is used for all Azure service authentication.
# No storage keys or SQL passwords needed.
EOF

    print_status "Configuration saved to: $CONFIG_FILE"
    echo ""
    echo "Configuration Summary:"
    echo "  Storage Account: $ACTUAL_STORAGE"
    echo "  Synapse Workspace: $ACTUAL_SYNAPSE"
    echo "  SQL Server: $ACTUAL_SQL"
    echo "  SQL Database: $SQL_DATABASE_NAME"
}

# =============================================================================
# Print Summary
# =============================================================================
print_summary() {
    print_header "Deployment Complete!"

    echo "Created Resources:"
    echo "  ✓ Data Lake (ADLS Gen2) with Private Endpoint"
    echo "  ✓ Synapse Analytics with Private Endpoint"
    echo "  ✓ Azure SQL Database with Private Endpoint"
    echo ""
    echo "Role Assignments:"
    echo "  ✓ VM Managed Identity → Storage Blob Data Contributor (Data Lake)"
    echo "  ✓ Synapse Managed Identity → Storage Blob Data Reader (Data Lake)"
    echo ""
    echo "Configuration file: $(realpath "$SCRIPT_DIR/../.env.generated")"
    echo ""
    echo "=============================================="
    echo "  Next Steps"
    echo "=============================================="
    echo ""
    echo "1. SSH to the VM using Azure CLI:"
    echo "   az ssh vm --resource-group $RESOURCE_GROUP --name $VM_NAME"
    echo ""
    echo "2. Copy files to the VM:"
    echo "   scp -r ../airflow ../scripts azureuser@<vm-ip>:/opt/airflow/"
    echo ""
    echo "3. On the VM, run Airflow setup:"
    echo "   cd /opt/airflow && docker compose up -d"
    echo ""
    echo "4. Grant VM Managed Identity access to Azure SQL:"
    echo "   Connect to the 'trinity' database as Azure AD admin and run:"
    echo "   CREATE USER [$VM_NAME] FROM EXTERNAL PROVIDER;"
    echo "   ALTER ROLE db_datareader ADD MEMBER [$VM_NAME];"
    echo "   ALTER ROLE db_datawriter ADD MEMBER [$VM_NAME];"
    echo "   GRANT CREATE TABLE TO [$VM_NAME];"
    echo ""
    echo "5. Configure Synapse credentials (run on VM):"
    echo "   python3 scripts/synapse_setup.py"
    echo ""
    echo "=============================================="
}

# =============================================================================
# Main
# =============================================================================
main() {
    echo ""
    echo "=============================================="
    echo "  Trinity Data Lakehouse Deployment"
    echo "=============================================="
    echo ""

    # Parse arguments
    SKIP_PREFLIGHT=false
    ONLY_CONFIG=false

    while [[ $# -gt 0 ]]; do
        case $1 in
            --skip-preflight)
                SKIP_PREFLIGHT=true
                shift
                ;;
            --only-config)
                ONLY_CONFIG=true
                shift
                ;;
            --help)
                echo "Usage: $0 [OPTIONS]"
                echo ""
                echo "Options:"
                echo "  --skip-preflight  Skip pre-flight checks"
                echo "  --only-config     Only generate config file (skip resource creation)"
                echo "  --help            Show this help message"
                exit 0
                ;;
            *)
                print_error "Unknown option: $1"
                exit 1
                ;;
        esac
    done

    # Run pre-flight checks
    if [ "$SKIP_PREFLIGHT" = false ]; then
        preflight_checks
    fi

    if [ "$ONLY_CONFIG" = true ]; then
        generate_config
        exit 0
    fi

    # Create resources
    create_datalake
    create_synapse
    create_sql

    # Generate configuration
    generate_config

    # Print summary
    print_summary
}

main "$@"
