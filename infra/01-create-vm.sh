#!/bin/bash
set -e

# Load variables
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/variables.sh"

echo "=== Azure Airflow VM Setup ==="
echo "Resource Group: $RESOURCE_GROUP"
echo "Location: $LOCATION"
echo "VM Size: $VM_SIZE"
echo ""

# Check if logged in to Azure
echo "Checking Azure login status..."
az account show > /dev/null 2>&1 || { echo "Please run 'az login' first"; exit 1; }

# Set subscription
echo "Setting subscription..."
az account set --subscription "$SUBSCRIPTION_ID"

# Generate SSH key if it doesn't exist
if [ ! -f "$SSH_KEY_PATH" ]; then
    echo "Generating SSH key..."
    ssh-keygen -t rsa -b 4096 -f "$SSH_KEY_PATH" -N "" -C "airflow-vm-key"
fi

# Create Resource Group
echo "Creating Resource Group..."
az group create \
    --name "$RESOURCE_GROUP" \
    --location "$LOCATION"

# Create Virtual Network
echo "Creating Virtual Network..."
az network vnet create \
    --resource-group "$RESOURCE_GROUP" \
    --name "$VNET_NAME" \
    --address-prefix 10.0.0.0/16 \
    --subnet-name "$SUBNET_NAME" \
    --subnet-prefix 10.0.1.0/24

# Create Network Security Group
echo "Creating Network Security Group..."
az network nsg create \
    --resource-group "$RESOURCE_GROUP" \
    --name "$NSG_NAME"

# Get current public IP for SSH restriction
MY_IP=$(curl -s https://ipinfo.io/ip)
echo "Your public IP: $MY_IP"

# Add NSG rules
echo "Adding NSG rules..."

# SSH - restricted to your IP
az network nsg rule create \
    --resource-group "$RESOURCE_GROUP" \
    --nsg-name "$NSG_NAME" \
    --name "AllowSSH" \
    --priority 1000 \
    --source-address-prefixes "$MY_IP/32" \
    --destination-port-ranges 22 \
    --access Allow \
    --protocol Tcp

# Airflow UI - restricted to your IP
az network nsg rule create \
    --resource-group "$RESOURCE_GROUP" \
    --nsg-name "$NSG_NAME" \
    --name "AllowAirflowUI" \
    --priority 1010 \
    --source-address-prefixes "$MY_IP/32" \
    --destination-port-ranges 8080 \
    --access Allow \
    --protocol Tcp

# Create Public IP
echo "Creating Public IP..."
az network public-ip create \
    --resource-group "$RESOURCE_GROUP" \
    --name "$PUBLIC_IP_NAME" \
    --sku Standard \
    --allocation-method Static

# Create NIC
echo "Creating Network Interface..."
az network nic create \
    --resource-group "$RESOURCE_GROUP" \
    --name "$NIC_NAME" \
    --vnet-name "$VNET_NAME" \
    --subnet "$SUBNET_NAME" \
    --network-security-group "$NSG_NAME" \
    --public-ip-address "$PUBLIC_IP_NAME"

# Create VM
echo "Creating Virtual Machine (this may take a few minutes)..."
az vm create \
    --resource-group "$RESOURCE_GROUP" \
    --name "$VM_NAME" \
    --nics "$NIC_NAME" \
    --image "$VM_IMAGE" \
    --size "$VM_SIZE" \
    --os-disk-size-gb "$OS_DISK_SIZE" \
    --storage-sku Premium_LRS \
    --admin-username "$ADMIN_USERNAME" \
    --ssh-key-values "$SSH_KEY_PATH.pub"

# Get public IP address
PUBLIC_IP=$(az network public-ip show \
    --resource-group "$RESOURCE_GROUP" \
    --name "$PUBLIC_IP_NAME" \
    --query ipAddress \
    --output tsv)

echo ""
echo "=== VM Created Successfully ==="
echo "Public IP: $PUBLIC_IP"
echo "SSH Command: ssh -i $SSH_KEY_PATH $ADMIN_USERNAME@$PUBLIC_IP"
echo ""
echo "Next steps:"
echo "1. Copy 02-setup-vm.sh and airflow/ folder to the VM"
echo "2. Run 02-setup-vm.sh on the VM to install Docker and Airflow"
echo ""
echo "Quick copy command:"
echo "scp -i $SSH_KEY_PATH -r ../airflow 02-setup-vm.sh $ADMIN_USERNAME@$PUBLIC_IP:~/"
