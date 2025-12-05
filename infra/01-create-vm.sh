#!/bin/bash
set -e

# Load variables
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/variables.sh"

echo "=== Azure Airflow VM Setup ==="
echo "Resource Group: $RESOURCE_GROUP"
echo "Location: $LOCATION"
echo "VM Size: $VM_SIZE"
echo "Allow All IPs: $ALLOW_ALL_IPS"
echo ""

# Check subscription ID is set
if [ -z "$SUBSCRIPTION_ID" ]; then
    echo "ERROR: SUBSCRIPTION_ID is not set."
    echo ""
    echo "Set it with:"
    echo "  export SUBSCRIPTION_ID=\"your-subscription-id\""
    echo ""
    echo "Or edit infra/variables.sh"
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

# Register required providers (learned from troubleshooting)
echo "Registering required Azure providers..."
az provider register --namespace Microsoft.Network --wait || true
az provider register --namespace Microsoft.Compute --wait || true

# Generate SSH key if it doesn't exist
if [ ! -f "$SSH_KEY_PATH" ]; then
    echo "Generating SSH key..."
    ssh-keygen -t rsa -b 4096 -f "$SSH_KEY_PATH" -N "" -C "airflow-vm-key"
fi

# Create Resource Group
echo "Creating Resource Group..."
az group create \
    --name "$RESOURCE_GROUP" \
    --location "$LOCATION" \
    --output none

# Create Virtual Network
echo "Creating Virtual Network..."
az network vnet create \
    --resource-group "$RESOURCE_GROUP" \
    --name "$VNET_NAME" \
    --address-prefix 10.0.0.0/16 \
    --subnet-name "$SUBNET_NAME" \
    --subnet-prefix 10.0.1.0/24 \
    --output none

# Create Network Security Group
echo "Creating Network Security Group..."
az network nsg create \
    --resource-group "$RESOURCE_GROUP" \
    --name "$NSG_NAME" \
    --output none

# Determine source IP for firewall rules
if [ "$ALLOW_ALL_IPS" = "true" ]; then
    SOURCE_IP="*"
    echo "Firewall: Allowing access from ANY IP"
else
    SOURCE_IP=$(curl -s https://ipinfo.io/ip)/32
    echo "Firewall: Restricting to your IP ($SOURCE_IP)"
fi

# Add NSG rules
echo "Adding NSG rules..."

# SSH
az network nsg rule create \
    --resource-group "$RESOURCE_GROUP" \
    --nsg-name "$NSG_NAME" \
    --name "AllowSSH" \
    --priority 1000 \
    --source-address-prefixes "$SOURCE_IP" \
    --destination-port-ranges 22 \
    --access Allow \
    --protocol Tcp \
    --output none

# Airflow UI
az network nsg rule create \
    --resource-group "$RESOURCE_GROUP" \
    --nsg-name "$NSG_NAME" \
    --name "AllowAirflowUI" \
    --priority 1010 \
    --source-address-prefixes "$SOURCE_IP" \
    --destination-port-ranges 8080 \
    --access Allow \
    --protocol Tcp \
    --output none

# Create Public IP
echo "Creating Public IP..."
az network public-ip create \
    --resource-group "$RESOURCE_GROUP" \
    --name "$PUBLIC_IP_NAME" \
    --sku Standard \
    --allocation-method Static \
    --output none

# Create NIC
echo "Creating Network Interface..."
az network nic create \
    --resource-group "$RESOURCE_GROUP" \
    --name "$NIC_NAME" \
    --vnet-name "$VNET_NAME" \
    --subnet "$SUBNET_NAME" \
    --network-security-group "$NSG_NAME" \
    --public-ip-address "$PUBLIC_IP_NAME" \
    --output none

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
    --ssh-key-values "$SSH_KEY_PATH.pub" \
    --output none

# Get public IP address
PUBLIC_IP=$(az network public-ip show \
    --resource-group "$RESOURCE_GROUP" \
    --name "$PUBLIC_IP_NAME" \
    --query ipAddress \
    --output tsv)

echo ""
echo "=============================================="
echo "         VM Created Successfully!            "
echo "=============================================="
echo ""
echo "Public IP: $PUBLIC_IP"
echo ""
echo "SSH Command:"
echo "  ssh -i $SSH_KEY_PATH $ADMIN_USERNAME@$PUBLIC_IP"
echo ""
echo "Next steps:"
echo "  1. Copy files to VM:"
echo "     scp -i $SSH_KEY_PATH -r ../airflow 02-setup-vm.sh $ADMIN_USERNAME@$PUBLIC_IP:~/"
echo ""
echo "  2. SSH into VM and run setup:"
echo "     ssh -i $SSH_KEY_PATH $ADMIN_USERNAME@$PUBLIC_IP"
echo "     chmod +x 02-setup-vm.sh && ./02-setup-vm.sh"
echo ""
echo "  3. Start Airflow:"
echo "     cd /opt/airflow && docker-compose up -d"
echo ""
echo "  4. Access Airflow UI:"
echo "     http://$PUBLIC_IP:8080"
echo ""
