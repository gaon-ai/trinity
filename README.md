# Trinity

A lean data lakehouse platform on Azure for BI and analytics.

## Architecture

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                              Azure (eastus2)                                │
│                                                                             │
│  ┌─────────────┐    ┌─────────────┐    ┌─────────────────────────────────┐  │
│  │             │    │             │    │     ADLS Gen2 (Data Lake)       │  │
│  │  Source DB  │───▶│   Airflow   │───▶│  ┌─────────┬────────┬───────┐  │  │
│  │   (ERP)     │    │    (VM)     │    │  │ Bronze  │ Silver │ Gold  │  │  │
│  │             │    │             │    │  │  (raw)  │(clean) │ (agg) │  │  │
│  └─────────────┘    └─────────────┘    │  └─────────┴────────┴───────┘  │  │
│                            │           └───────────────┬────────────────┘  │
│                            │                           │                   │
│                            ▼                           ▼                   │
│                     ┌ ─ ─ ─ ─ ─ ─ ┐          ┌─────────────────┐          │
│                       Azure SQL              │     Synapse     │          │
│                     │ (optional)  │          │   (Serverless)  │◀── SQL   │
│                      ─ ─ ─ ─ ─ ─ ─           └────────┬────────┘          │
│                            │                          │                   │
│                            └──────────┬───────────────┘                   │
│                                       ▼                                   │
│                               ┌─────────────┐                             │
│                               │  Power BI   │                             │
│                               └─────────────┘                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

| Layer | Component | Purpose |
|-------|-----------|---------|
| Orchestration | Airflow on Azure VM | Schedule and run data pipelines |
| Storage | ADLS Gen2 (Bronze/Silver/Gold) | Medallion architecture data lake |
| Query | Synapse Serverless SQL | Ad-hoc SQL queries on Data Lake |
| Serving | Azure SQL *(optional)* | Fast queries for Power BI DirectQuery |
| BI | Power BI | Business intelligence dashboards |

## Quick Start

### Prerequisites

- Azure CLI: `brew install azure-cli`
- Azure subscription with Contributor access
- Docker (for local development)

### Deploy Infrastructure

```bash
# 1. Login and set subscription
az login
export SUBSCRIPTION_ID="your-subscription-id"
export RESOURCE_GROUP="trinity-rg"
export LOCATION="eastus2"
export VM_NAME="airflow-vm"
export VNET_NAME="trinity-vnet"
export SUBNET_NAME="trinity-subnet"

# 2. Create resources (run from repo root)
./scripts/01-create-vm.sh           # Creates VM, VNet, NSG
./scripts/03-create-datalake.sh     # Creates ADLS Gen2
./scripts/04-create-synapse.sh      # Creates Synapse Serverless

# 3. Setup Airflow on VM
scp -i ~/.ssh/airflow_vm_key -r airflow scripts/02-setup-vm.sh azureuser@<VM_IP>:~/
ssh -i ~/.ssh/airflow_vm_key azureuser@<VM_IP>
chmod +x 02-setup-vm.sh && ./02-setup-vm.sh

# 4. Configure Synapse
python3 scripts/synapse_setup.py
```

See `scripts/README.md` for detailed script documentation.

## Project Structure

```
trinity/
├── .github/workflows/
│   └── deploy-airflow.yml      # CI/CD: auto-deploy DAGs on push
├── airflow/
│   ├── Dockerfile              # Custom Airflow image
│   ├── docker-compose.yaml     # Airflow services
│   ├── .env.example            # Environment template
│   └── dags/
│       ├── hello_world_dag.py      # Test DAG
│       └── datalake_sample_dag.py  # Bronze → Silver → Gold pipeline
├── scripts/
│   ├── README.md               # Script documentation
│   ├── variables.sh            # Configuration
│   ├── 01-create-vm.sh         # Create VM infrastructure
│   ├── 02-setup-vm.sh          # Setup Docker/Airflow on VM
│   ├── 03-create-datalake.sh   # Create Data Lake
│   ├── 04-create-synapse.sh    # Create Synapse
│   ├── 05-create-azure-sql.sh  # Create Azure SQL (optional)
│   ├── synapse_setup.py        # Configure Synapse credentials
│   ├── local-dev.sh            # Local Airflow development
│   └── validate_dags.py        # DAG syntax validation
└── docs/                       # Local docs with credentials (gitignored)
```

## Local Development

```bash
# Start local Airflow
./scripts/local-dev.sh start

# Access at http://localhost:8080 (admin/admin)

# Validate DAGs before committing
./scripts/local-dev.sh test

# Stop
./scripts/local-dev.sh stop
```

## CI/CD

DAGs automatically deploy to the VM when pushing to `main`:

1. **Validate** - Python syntax check on all DAGs
2. **Deploy** - rsync DAGs to VM
3. **Rebuild** - Rebuild containers if Dockerfile/compose changed

### GitHub Secrets Required

| Secret | Description |
|--------|-------------|
| `AIRFLOW_VM_IP` | VM public IP address |
| `AIRFLOW_SSH_USER` | SSH username (azureuser) |
| `AIRFLOW_SSH_KEY` | Contents of SSH private key |

## Data Pipeline

The `datalake_sample` DAG demonstrates the medallion architecture:

```
Bronze (raw JSON) → Silver (cleaned CSV) → Gold (aggregates) → SQL (optional)
```

| Layer | Path | Format |
|-------|------|--------|
| Bronze | `bronze/erp/sales/date=YYYY-MM-DD/` | JSON |
| Silver | `silver/erp/sales/date=YYYY-MM-DD/` | CSV |
| Gold | `gold/analytics/sales_by_region/` | CSV |
| Gold | `gold/serving/daily_summary/` | JSON |

## Synapse Queries

Query Data Lake files directly with SQL:

```sql
-- Must use 'trinity' database, not 'master'
SELECT *
FROM OPENROWSET(
    BULK 'silver/erp/sales/*/sales_cleaned.csv',
    DATA_SOURCE = 'TrinityLake',
    FORMAT = 'CSV',
    PARSER_VERSION = '2.0',
    FIRSTROW = 2
) WITH (
    order_id VARCHAR(50),
    product VARCHAR(100),
    quantity INT,
    region VARCHAR(50),
    total_amount DECIMAL(10,2)
) AS sales
```

## Costs (Estimated Monthly)

| Resource | Cost |
|----------|------|
| VM (D4s_v3) | ~$190 |
| Data Lake | ~$10-30 |
| Synapse | ~$7/TB scanned |
| Azure SQL (optional) | ~$20 |
| **Total** | **~$200-250** |

## Cleanup

```bash
# Delete all resources
az group delete --name trinity-rg --yes --no-wait
```

## Documentation

Detailed documentation with credentials is in `docs/TRINITY-GUIDE.md` (local only, gitignored).

## License

MIT
