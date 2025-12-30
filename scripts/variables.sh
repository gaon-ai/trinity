#!/bin/bash
# Azure Infrastructure Configuration
# Update these values before running the scripts

# =============================================================================
# REQUIRED: Set these before running infrastructure scripts
# =============================================================================
export SUBSCRIPTION_ID="${SUBSCRIPTION_ID:-}"      # Azure subscription ID
export RESOURCE_GROUP="${RESOURCE_GROUP:-}"        # Resource group name
export VM_NAME="${VM_NAME:-}"                      # VM name (created by infra team)
export VNET_NAME="${VNET_NAME:-}"                  # VNet name (created by infra team)
export SUBNET_NAME="${SUBNET_NAME:-}"              # Subnet for Private Endpoints
export VM_IDENTITY_ID="${VM_IDENTITY_ID:-}"        # VM Managed Identity principal ID

# =============================================================================
# Azure Settings
# =============================================================================
export LOCATION="${LOCATION:-eastus2}"

# =============================================================================
# VM Settings
# =============================================================================
export ADMIN_USERNAME="${ADMIN_USERNAME:-azureuser}"
export VM_SIZE="${VM_SIZE:-Standard_D4s_v3}"
export VM_IMAGE="${VM_IMAGE:-Ubuntu2204}"
export OS_DISK_SIZE="${OS_DISK_SIZE:-128}"
export SSH_KEY_PATH="${SSH_KEY_PATH:-$HOME/.ssh/airflow_vm_key}"
export ALLOW_ALL_IPS="${ALLOW_ALL_IPS:-true}"

# Derived names (based on VM_NAME)
export NSG_NAME="${NSG_NAME:-${VM_NAME}-nsg}"
export PUBLIC_IP_NAME="${PUBLIC_IP_NAME:-${VM_NAME}-pip}"
export NIC_NAME="${NIC_NAME:-${VM_NAME}-nic}"

# =============================================================================
# Data Lake Settings
# =============================================================================
# Storage account name must be globally unique, 3-24 chars, lowercase letters and numbers only
export STORAGE_ACCOUNT_NAME="${STORAGE_ACCOUNT_NAME:-trinitylake}"

# =============================================================================
# Synapse Analytics Settings
# =============================================================================
# Synapse workspace name must be globally unique, lowercase
export SYNAPSE_WORKSPACE_NAME="${SYNAPSE_WORKSPACE_NAME:-trinitysynapse}"
export SYNAPSE_SQL_ADMIN="${SYNAPSE_SQL_ADMIN:-sqladmin}"
# Password will be auto-generated if not set
export SYNAPSE_SQL_PASSWORD="${SYNAPSE_SQL_PASSWORD:-}"

# =============================================================================
# Azure SQL Database Settings
# =============================================================================
# SQL Server name must be globally unique, lowercase
export SQL_SERVER_NAME="${SQL_SERVER_NAME:-trinitydb}"
export SQL_DATABASE_NAME="${SQL_DATABASE_NAME:-trinity}"
# For Azure AD Only auth, this is just for initial setup
export SQL_ADMIN_USER="${SQL_ADMIN_USER:-sqladmin}"
# Password will be auto-generated if not set
export SQL_ADMIN_PASSWORD="${SQL_ADMIN_PASSWORD:-}"
# SKU: Basic (~$5/mo), S0 (~$15/mo), S1 (~$30/mo), S2 (~$75/mo)
export SQL_SKU="${SQL_SKU:-S0}"

# =============================================================================
# Private Endpoint Settings
# =============================================================================
export PE_SUBNET_NAME="${PE_SUBNET_NAME:-${SUBNET_NAME}}"  # Subnet for Private Endpoints

# =============================================================================
# Validation Function
# =============================================================================
validate_required_vars() {
    local missing=()

    [[ -z "$SUBSCRIPTION_ID" ]] && missing+=("SUBSCRIPTION_ID")
    [[ -z "$RESOURCE_GROUP" ]] && missing+=("RESOURCE_GROUP")
    [[ -z "$VM_NAME" ]] && missing+=("VM_NAME")
    [[ -z "$VNET_NAME" ]] && missing+=("VNET_NAME")
    [[ -z "$SUBNET_NAME" ]] && missing+=("SUBNET_NAME")

    if [[ ${#missing[@]} -gt 0 ]]; then
        echo "ERROR: Missing required variables:"
        for var in "${missing[@]}"; do
            echo "  - $var"
        done
        echo ""
        echo "These should be provided by the infrastructure team."
        echo "Example:"
        echo "  export SUBSCRIPTION_ID=\"your-sub-id\""
        echo "  export RESOURCE_GROUP=\"airflow-rg\""
        echo "  export VM_NAME=\"airflow-vm\""
        echo "  export VNET_NAME=\"airflow-vnet\""
        echo "  export SUBNET_NAME=\"airflow-subnet\""
        return 1
    fi

    return 0
}
