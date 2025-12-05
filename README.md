# Trinity

Airflow deployment on Azure VM with Docker Compose.

## Prerequisites

- [Azure CLI](https://docs.microsoft.com/en-us/cli/azure/install-azure-cli) installed
- Azure subscription with permissions to create resources

## Quick Start

### 1. Clone the repository

```bash
git clone https://github.com/gaon-ai/trinity.git
cd trinity
```

### 2. Login to Azure

```bash
az login
```

### 3. Create the VM

```bash
cd infra
chmod +x *.sh
./01-create-vm.sh
```

This will create:
- Resource Group: `airflow-rg`
- VM: `Standard_D4s_v3` (4 vCPU, 16 GB RAM)
- Location: `eastus2`
- Ubuntu 22.04 LTS with 128 GB SSD

### 4. Setup the VM

Copy files to the VM and run setup:

```bash
# Copy files (command shown at end of step 3)
scp -i ~/.ssh/airflow_vm_key -r ../airflow 02-setup-vm.sh azureuser@<VM_IP>:~/

# SSH into the VM
ssh -i ~/.ssh/airflow_vm_key azureuser@<VM_IP>

# Run setup script
chmod +x 02-setup-vm.sh
./02-setup-vm.sh
```

### 5. Start Airflow

```bash
cd /opt/airflow
docker-compose up -d
```

### 6. Access Airflow

Open `http://<VM_IP>:8080` in your browser.

Default credentials are displayed during setup (check the output of `02-setup-vm.sh`).

## Project Structure

```
trinity/
├── infra/
│   ├── variables.sh        # Configuration variables
│   ├── 01-create-vm.sh     # Creates Azure VM
│   └── 02-setup-vm.sh      # Configures VM with Docker
├── airflow/
│   ├── docker-compose.yaml # Airflow services
│   ├── .env.example        # Environment template
│   └── dags/
│       └── example_dag.py  # Sample DAG
└── README.md
```

## Configuration

### VM Settings

Edit `infra/variables.sh` to customize:
- VM size
- Region
- Resource names

### Airflow Settings

Edit `airflow/.env` on the VM to configure:
- Admin credentials
- Additional Python packages
- Fernet key

## Costs

Estimated monthly cost: ~$163
- VM (Standard_D4s_v3): ~$140
- Storage (128 GB Premium SSD): ~$20
- Static IP: ~$3

## Cleanup

To delete all resources:

```bash
az group delete --name airflow-rg --yes --no-wait
```

## License

MIT
