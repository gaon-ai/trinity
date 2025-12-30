#!/bin/bash
# Simulate what Infrastructure Team provides
#
# This script creates the resources that the infra team would provide:
# - Resource Group
# - VNet + Subnet
# - VM with public IP and System Managed Identity
#
# After this runs, we can test deploy.sh as if infra team provided these resources.

set -e

# Load configuration
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/variables.sh"

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

print_status() { echo -e "${GREEN}[✓]${NC} $1"; }
print_warning() { echo -e "${YELLOW}[!]${NC} $1"; }
print_error() { echo -e "${RED}[✗]${NC} $1"; }

echo ""
echo "=============================================="
echo "  Simulating Infrastructure Team Setup"
echo "=============================================="
echo ""
echo "This creates what infra team would provide:"
echo "  - Resource Group"
echo "  - VNet + Subnet"
echo "  - VM with Public IP + Managed Identity"
echo ""

# Check subscription
if [ -z "$SUBSCRIPTION_ID" ]; then
    print_error "SUBSCRIPTION_ID not set"
    echo "Please run: export SUBSCRIPTION_ID=\"your-subscription-id\""
    exit 1
fi

# Check Azure login
if ! az account show &> /dev/null; then
    print_error "Not logged in to Azure. Please run: az login"
    exit 1
fi

# Set subscription
az account set --subscription "$SUBSCRIPTION_ID"
print_status "Using subscription: $SUBSCRIPTION_ID"

# Set defaults for testing
RESOURCE_GROUP="${RESOURCE_GROUP:-trinity-test-rg}"
LOCATION="${LOCATION:-eastus2}"
VM_NAME="${VM_NAME:-trinity-airflow-vm}"
VNET_NAME="${VNET_NAME:-trinity-vnet}"
SUBNET_NAME="${SUBNET_NAME:-trinity-subnet}"
ADMIN_USERNAME="${ADMIN_USERNAME:-azureuser}"
VM_SIZE="${VM_SIZE:-Standard_D2s_v3}"  # Smaller for testing
VM_IMAGE="Ubuntu2204"
NSG_NAME="${VM_NAME}-nsg"
PUBLIC_IP_NAME="${VM_NAME}-ip"
NIC_NAME="${VM_NAME}-nic"
SSH_KEY_PATH="$HOME/.ssh/trinity_vm_key"

echo ""
echo "Configuration:"
echo "  Resource Group: $RESOURCE_GROUP"
echo "  Location: $LOCATION"
echo "  VM Name: $VM_NAME"
echo "  VNet: $VNET_NAME"
echo "  Subnet: $SUBNET_NAME"
echo ""

# Register providers
echo "Registering providers..."
az provider register --namespace Microsoft.Network --wait 2>/dev/null || true
az provider register --namespace Microsoft.Compute --wait 2>/dev/null || true
print_status "Providers registered"

# Create Resource Group
echo "Creating Resource Group..."
az group create \
    --name "$RESOURCE_GROUP" \
    --location "$LOCATION" \
    --output none
print_status "Resource Group: $RESOURCE_GROUP"

# Create Virtual Network
echo "Creating VNet and Subnet..."
az network vnet create \
    --resource-group "$RESOURCE_GROUP" \
    --name "$VNET_NAME" \
    --address-prefix 10.0.0.0/16 \
    --subnet-name "$SUBNET_NAME" \
    --subnet-prefix 10.0.1.0/24 \
    --output none
print_status "VNet: $VNET_NAME, Subnet: $SUBNET_NAME"

# Create NSG with SSH only
echo "Creating NSG (SSH only)..."
az network nsg create \
    --resource-group "$RESOURCE_GROUP" \
    --name "$NSG_NAME" \
    --output none

# Get current IP for SSH access
MY_IP=$(curl -s https://ipinfo.io/ip 2>/dev/null || echo "*")
if [ "$MY_IP" != "*" ]; then
    MY_IP="${MY_IP}/32"
fi

az network nsg rule create \
    --resource-group "$RESOURCE_GROUP" \
    --nsg-name "$NSG_NAME" \
    --name "AllowSSH" \
    --priority 1000 \
    --source-address-prefixes "$MY_IP" \
    --destination-port-ranges 22 \
    --access Allow \
    --protocol Tcp \
    --output none
print_status "NSG: SSH allowed from $MY_IP"

# Create Public IP
echo "Creating Public IP..."
az network public-ip create \
    --resource-group "$RESOURCE_GROUP" \
    --name "$PUBLIC_IP_NAME" \
    --sku Standard \
    --allocation-method Static \
    --output none
print_status "Public IP created"

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
print_status "NIC created"

# Generate SSH key if needed
if [ ! -f "$SSH_KEY_PATH" ]; then
    echo "Generating SSH key..."
    ssh-keygen -t rsa -b 4096 -f "$SSH_KEY_PATH" -N "" -C "trinity-vm-key"
    print_status "SSH key generated: $SSH_KEY_PATH"
fi

# Create VM with Managed Identity
echo "Creating VM with Managed Identity (this takes a few minutes)..."
az vm create \
    --resource-group "$RESOURCE_GROUP" \
    --name "$VM_NAME" \
    --nics "$NIC_NAME" \
    --image "$VM_IMAGE" \
    --size "$VM_SIZE" \
    --os-disk-size-gb 128 \
    --storage-sku Premium_LRS \
    --admin-username "$ADMIN_USERNAME" \
    --ssh-key-values "$SSH_KEY_PATH.pub" \
    --assign-identity \
    --output none
print_status "VM created with System Managed Identity"

# Get VM details
PUBLIC_IP=$(az network public-ip show \
    --resource-group "$RESOURCE_GROUP" \
    --name "$PUBLIC_IP_NAME" \
    --query ipAddress -o tsv)

VM_IDENTITY_ID=$(az vm identity show \
    --resource-group "$RESOURCE_GROUP" \
    --name "$VM_NAME" \
    --query principalId -o tsv)

echo ""
echo "=============================================="
echo "  Infrastructure Ready!"
echo "=============================================="
echo ""
echo "What was created (simulating infra team):"
echo "  Resource Group: $RESOURCE_GROUP"
echo "  VNet: $VNET_NAME"
echo "  Subnet: $SUBNET_NAME"
echo "  VM Name: $VM_NAME"
echo "  VM Public IP: $PUBLIC_IP"
echo "  VM Managed Identity: $VM_IDENTITY_ID"
echo ""
echo "SSH to VM:"
echo "  ssh -i $SSH_KEY_PATH $ADMIN_USERNAME@$PUBLIC_IP"
echo ""
echo "=============================================="
echo "  Now run deploy.sh to create Data Lake, Synapse, SQL"
echo "=============================================="
echo ""
echo "export SUBSCRIPTION_ID=\"$SUBSCRIPTION_ID\""
echo "export RESOURCE_GROUP=\"$RESOURCE_GROUP\""
echo "export VM_NAME=\"$VM_NAME\""
echo "export VNET_NAME=\"$VNET_NAME\""
echo "export SUBNET_NAME=\"$SUBNET_NAME\""
echo ""
echo "./infra/deploy.sh"
echo ""
