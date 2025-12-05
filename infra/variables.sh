#!/bin/bash
# Azure Infrastructure Configuration
# Update these values before running the scripts

# =============================================================================
# REQUIRED: Set your subscription ID before running any scripts
# =============================================================================
export SUBSCRIPTION_ID="${SUBSCRIPTION_ID:-}"  # Set via: export SUBSCRIPTION_ID="your-id"

# =============================================================================
# Azure Settings
# =============================================================================
export RESOURCE_GROUP="airflow-rg"
export LOCATION="eastus2"

# =============================================================================
# VM Settings
# =============================================================================
export VM_NAME="airflow-vm"
export VM_SIZE="Standard_D4s_v3"  # 4 vCPU, 16 GB RAM (~$140/month)
export VM_IMAGE="Canonical:0001-com-ubuntu-server-jammy:22_04-lts-gen2:latest"
export OS_DISK_SIZE=128
export ADMIN_USERNAME="azureuser"

# =============================================================================
# Network Settings
# =============================================================================
export VNET_NAME="airflow-vnet"
export SUBNET_NAME="airflow-subnet"
export NSG_NAME="airflow-nsg"
export PUBLIC_IP_NAME="airflow-pip"
export NIC_NAME="airflow-nic"

# Firewall: Set to "true" to allow access from any IP (less secure but convenient)
# Set to "false" to restrict to your current IP only
export ALLOW_ALL_IPS="${ALLOW_ALL_IPS:-true}"

# =============================================================================
# SSH Key Path (will be generated if doesn't exist)
# =============================================================================
export SSH_KEY_PATH="$HOME/.ssh/airflow_vm_key"

# =============================================================================
# Data Lake Settings
# =============================================================================
# Storage account name must be globally unique, 3-24 chars, lowercase letters and numbers only
# A random suffix will be added if the name is taken
export STORAGE_ACCOUNT_NAME="${STORAGE_ACCOUNT_NAME:-trinitylake}"
