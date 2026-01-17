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

# 2. Create resources (run from infra/)
cd infra
./01-create-vm.sh           # Creates VM, VNet, NSG
./03-create-datalake.sh     # Creates ADLS Gen2
./04-create-synapse.sh      # Creates Synapse Serverless

# 3. Setup Airflow on VM
scp -i ~/.ssh/airflow_vm_key -r ../airflow 02-setup-vm.sh azureuser@<VM_IP>:~/
ssh -i ~/.ssh/airflow_vm_key azureuser@<VM_IP>
chmod +x 02-setup-vm.sh && ./02-setup-vm.sh

# 4. Configure Synapse views
python3 scripts/synapse_setup.py
```

See `docs/` for detailed setup documentation.

## Project Structure

```
trinity/
├── .github/workflows/
│   └── deploy-airflow.yml         # CI/CD: auto-deploy DAGs on push
├── airflow/
│   ├── Dockerfile                 # Custom Airflow image
│   ├── docker-compose.yaml        # Airflow services
│   ├── .env.example               # Environment template
│   ├── dags/
│   │   ├── bronze/
│   │   │   ├── sales_ingestion.py         # Bronze: ERP sales data
│   │   │   └── client_inventory_ingestion.py  # Bronze: Client SQL Server
│   │   ├── silver/
│   │   │   └── sales_transform.py         # Silver: clean & transform
│   │   └── gold/
│   │       ├── sales_aggregation.py       # Gold: ERP aggregates
│   │       └── client_facts.py            # Gold: Client fact tables
│   └── plugins/
│       ├── lib/
│       │   ├── datasets.py        # Airflow Dataset definitions
│       │   ├── datalake.py        # Azure Data Lake operations
│       │   ├── client_db.py       # Client SQL Server operations
│       │   ├── sql_templates.py   # Jinja2 SQL templating
│       │   ├── tables/            # Table configurations
│       │   │   └── client/        # Client table definitions
│       │   └── transformations/   # Gold layer transformations
│       │       ├── _constants.py  # Business constants
│       │       ├── _helpers.py    # Shared helpers
│       │       └── client/        # Client-specific transforms
│       └── sql/                   # SQL templates
│           └── sqlserver/bronze/  # Bronze query templates
├── scripts/
│   ├── variables.sh               # Configuration
│   ├── 01-create-vm.sh            # Create VM infrastructure
│   ├── 02-setup-vm.sh             # Setup Docker/Airflow on VM
│   ├── 03-create-datalake.sh      # Create Data Lake
│   ├── 04-create-synapse.sh       # Create Synapse
│   ├── 05-create-azure-sql.sh     # Create Azure SQL (optional)
│   ├── synapse_setup.py           # Configure Synapse credentials
│   ├── local-dev.sh               # Local Airflow development
│   └── validate_dags.py           # DAG syntax validation
└── docs/                          # Documentation
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

Multi-DAG medallion architecture with Dataset-based triggers.

### Client Data Pipeline (Bronze → Gold)

```
┌───────────────────────────┐    Dataset      ┌─────────────────────────┐
│  bronze_client_inventory  │─BRONZE_CLIENT───▶│  gold_client_facts      │
│  _ingestion               │                  │                         │
│                           │                  │  • fact_invoice         │
│  • invoice_item           │                  │  • fact_invoice_detail  │
│  • invoice_header         │                  │  • fact_order_item      │
│  • order_item             │                  │  • dim_customer         │
│  • customer_address       │                  │  • margin_invoice       │
│  • general_ledger         │                  │  • fact_general_ledger  │
│                           │                  │  • margin_rebate        │
└───────────────────────────┘                  └─────────────────────────┘
        @daily                                    On BRONZE_CLIENT_DATA
```

| Gold Table | Description | Source |
|------------|-------------|--------|
| fact_invoice | Invoice line items with margins | invoice_item |
| fact_invoice_detail | Invoice to customer mapping | invoice_header |
| fact_order_item | Order details | order_item |
| dim_customer | Customer dimension (normalized names) | customer_address |
| margin_invoice | Denormalized reporting table | All tables joined |
| fact_general_ledger | General ledger (sales rebate) | general_ledger |
| margin_rebate | Aggregated rebates by customer | general_ledger |

### ERP Sales Pipeline (Bronze → Silver → Gold)

```
┌───────────────────┐    Dataset     ┌──────────────────┐    Dataset     ┌─────────────────────┐
│  bronze_sales_    │───BRONZE_SALES──▶│  silver_sales_   │───SILVER_SALES──▶│  gold_sales_        │
│  ingestion        │                │  transform       │                │  aggregation        │
│                   │                │                  │                │                     │
│  • Sensor         │                │  • Wait for file │                │  • By region        │
│  • Ingest raw     │                │  • Transform     │                │  • By product       │
│  • Publish dataset│                │  • Publish dataset│               │  • Daily summary    │
└───────────────────┘                └──────────────────┘                └─────────────────────┘
     @hourly                           On BRONZE_SALES                      On SILVER_SALES
```

**Key Concepts:**
- **Sensor**: Bronze DAG waits for source system availability
- **Dataset**: DAGs publish events when data is written (`outlets=[DATASET]`)
- **Trigger**: Downstream DAGs automatically run when upstream Dataset updates

### Data Lake Paths

| Layer | Path | Format |
|-------|------|--------|
| Bronze (Client) | `bronze/client/{table}/{YYYY-MM-DD-HH}/data.json` | JSON |
| Bronze (ERP) | `bronze/erp/sales/{YYYY-MM-DD-HH}/raw.json` | JSON |
| Silver | `silver/sales/{YYYY-MM-DD-HH}/cleaned.csv` | CSV |
| Gold | `gold/{table}/{YYYY-MM-DD-HH}/data.csv` | CSV |

## Synapse Queries

Query gold tables using OPENROWSET with wildcards (matches all partitions):

```sql
-- Client Gold Tables
SELECT * FROM OPENROWSET(
    BULK 'https://<storage>.dfs.core.windows.net/gold/fact_invoice/*/data.csv',
    FORMAT = 'CSV', PARSER_VERSION = '2.0', HEADER_ROW = TRUE
) AS fact_invoice;

SELECT * FROM OPENROWSET(
    BULK 'https://<storage>.dfs.core.windows.net/gold/margin_invoice/*/data.csv',
    FORMAT = 'CSV', PARSER_VERSION = '2.0', HEADER_ROW = TRUE
) AS margin_invoice;

SELECT * FROM OPENROWSET(
    BULK 'https://<storage>.dfs.core.windows.net/gold/margin_rebate/*/data.csv',
    FORMAT = 'CSV', PARSER_VERSION = '2.0', HEADER_ROW = TRUE
) AS margin_rebate;
```

Or use views created by `scripts/synapse_setup.py`:

```sql
USE trinity;
SELECT * FROM sales_by_region;    -- Revenue by region
SELECT * FROM sales_by_product;   -- Revenue by product
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

- `docs/infrastructure-request.md` - Azure resource specifications
- `docs/replication-guide.md` - Steps to replicate deployment
- `docs/cost-estimate.md` - Monthly cost breakdown

Note: Credentials should be stored in `airflow/.env` (gitignored).

## License

MIT
