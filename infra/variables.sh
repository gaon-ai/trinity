#!/bin/bash
# Azure VM Configuration Variables
# Update these values before running the scripts

# Azure Settings
export SUBSCRIPTION_ID="a41e49dd-5828-40a3-837c-cf2327a4988c"
export RESOURCE_GROUP="airflow-rg"
export LOCATION="eastus2"

# VM Settings
export VM_NAME="airflow-vm"
export VM_SIZE="Standard_D4s_v3"
export VM_IMAGE="Canonical:0001-com-ubuntu-server-jammy:22_04-lts-gen2:latest"
export OS_DISK_SIZE=128
export ADMIN_USERNAME="azureuser"

# Network Settings
export VNET_NAME="airflow-vnet"
export SUBNET_NAME="airflow-subnet"
export NSG_NAME="airflow-nsg"
export PUBLIC_IP_NAME="airflow-pip"
export NIC_NAME="airflow-nic"

# SSH Key Path (will be generated if doesn't exist)
export SSH_KEY_PATH="$HOME/.ssh/airflow_vm_key"
