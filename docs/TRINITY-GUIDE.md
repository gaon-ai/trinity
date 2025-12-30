# Trinity Data Lakehouse

> Complete guide for deploying and operating the Trinity data lakehouse platform on Azure.

---

## Architecture Overview

```
┌──────────────────────────────────────────────────────────────────────────────┐
│                         Azure VNet (East US 2)                          │
│                         trinity-vnet (10.0.0.0/16)                           │
│                                                                              │
│  ┌─────────────────────────────────────────────────────────────────────────┐ │
│  │                    Subnet: trinity-subnet (10.0.1.0/24)                 │ │
│  │                                                                         │ │
│  │  ┌─────────────┐                                                        │ │
│  │  │   Airflow   │         ┌────────────────────────────────────────────┐ │ │
│  │  │    (VM)     │────────▶│            ADLS Gen2 (Data Lake)           │ │ │
│  │  │             │  WRITE  │  ┌──────────┐ ┌──────────┐ ┌──────────┐   │ │ │
│  │  │  Port 22    │         │  │  Bronze  │▶│  Silver  │▶│   Gold   │   │ │ │
│  │  │  (SSH)      │         │  │  (raw)   │ │ (clean)  │ │  (agg)   │   │ │ │
│  │  └──────┬──────┘         │  └──────────┘ └──────────┘ └────┬─────┘   │ │ │
│  │         │                └─────────────────────────────────│─────────┘ │ │
│  │         :                              Private Endpoint ◄──┘           │ │
│  │         : OPTIONAL                                                     │ │
│  │         ▼                                                              │ │
│  │  ┌ ─ ─ ─ ─ ─ ─ ┐                              ┌─────────────────┐      │ │
│  │    Azure SQL    ◀╌╌ Power BI                  │     Synapse     │◀─┐   │ │
│  │  │ (optional)  │    (DirectQuery)             │   (Serverless)  │  │   │ │
│  │   ─ ─ ─ ─ ─ ─ ─                               └─────────────────┘  │   │ │
│  │                                                                    │   │ │
│  └────────────────────────────────────────────────────────────────────│───┘ │
└───────────────────────────────────────────────────────────────────────│─────┘
                                                                        │
        ┌───────────────────────────────────────────────────────────────┘
        │
        │              ┌─────────────┐
        │              │  Power BI   │  (Import mode from Synapse)
        │              │  Service    │
        │              └─────────────┘
        │
   ┌────┴────┐               ┌─────────────┐
   │  Your   │───SSH:22─────▶│   Airflow   │
   │ Laptop  │               │     VM      │
   │         │◀──Tunnel:8080─│             │
   └─────────┘               └─────────────┘
```

### Components

| Component | What It Does |
|-----------|--------------|
| **Airflow VM** | Orchestrates data pipelines. Runs Docker containers (scheduler, webserver, PostgreSQL). Executes DAGs that move data through Bronze → Silver → Gold layers. Accessed via SSH. |
| **Data Lake (ADLS Gen2)** | Stores all data in a hierarchical file system. Bronze holds raw ingested data, Silver holds cleaned/validated data, Gold holds aggregated business-ready data. Uses Parquet/CSV/JSON formats. |
| **Synapse Serverless** | SQL query engine that reads directly from Data Lake files. No data copying—queries run on-demand against Gold layer. Pay per TB scanned (~$7/TB). Ideal for ad-hoc analysis and Power BI Import mode. |
| **Azure SQL** *(optional)* | Traditional database for pre-materialized aggregates. Add only if you need fast DirectQuery dashboards or scan costs exceed ~$20/mo. The DAG automatically skips SQL load if not configured. |
| **Private Endpoints** | Secure networking—all Azure services communicate within the VNet, not over public internet. Each service gets a private IP in the subnet. |

### Data Flow

```
                                ┌─────────────────────────────────────────┐
                                │              Power BI                   │
                                │  ┌─────────────────────────────────┐    │
                                │  │ Option A: Import from Synapse   │◀───┼──── Recommended
                                │  │ Option B: DirectQuery to SQL    │◀╌╌╌┼╌╌╌╌ If needed
                                │  └─────────────────────────────────┘    │
                                └─────────────────────────────────────────┘
                                                    ▲
                                                    │
┌─────────┐    ┌─────────┐    ┌─────────┐    ┌─────────┐    ┌ ─ ─ ─ ─ ─ ┐
│ Source  │───▶│ Bronze  │───▶│ Silver  │───▶│  Gold   │╌╌╌▶ Azure SQL
│  Data   │    │  (raw)  │    │ (clean) │    │  (agg)  │    │(optional) │
└─────────┘    └─────────┘    └─────────┘    └─────────┘    └ ─ ─ ─ ─ ─ ┘
                                                  │
                                                  ▼
                                          ┌─────────────┐
                                          │   Synapse   │
                                          │  (ad-hoc)   │
                                          └─────────────┘
```

---

## Network & Connectivity

### Network Topology

| Resource | Name | CIDR/Address |
|----------|------|--------------|
| VNet | `trinity-vnet` | `10.0.0.0/16` |
| Subnet | `trinity-subnet` | `10.0.1.0/24` |
| VM Private IP | `airflow-vm` | `10.0.1.4` (dynamic) |
| VM Public IP | `airflow-vm-ip` | Static (assigned at creation) |

### NSG (Network Security Group) Rules

| Rule | Port | Source | Purpose |
|------|------|--------|---------|
| AllowSSH | 22 | Your IP or `*` | SSH access (also used for Airflow UI tunnel) |

> **Security Note:** Port 8080 is NOT exposed. Airflow UI is accessed via SSH tunnel only. For production, restrict SSH source IPs.

### Private Endpoints

All Azure services communicate via Private Endpoints within the VNet:

| Service | Private Endpoint | DNS Zone | Required |
|---------|-----------------|----------|----------|
| Data Lake | `pe-trinitylake` | `privatelink.dfs.core.windows.net` | Yes |
| Synapse | `pe-trinitysynapse` | `privatelink.sql.azuresynapse.net` | Yes |
| Azure SQL | `pe-trinitydb` | `privatelink.database.windows.net` | Optional |

---

## Connecting to Airflow

Airflow UI is **not exposed publicly** for security. Access is via SSH tunnel only.

### SSH Tunnel (Secure Access)

```
┌──────────────┐          ┌──────────────┐          ┌──────────────┐
│  Your Laptop │──SSH:22─▶│   Azure VM   │          │   Airflow    │
│              │          │              │──local──▶│  Container   │
│  localhost   │◀─────────│   forwards   │          │  Port 8080   │
│  :8080       │  tunnel  │   port 8080  │          │              │
└──────────────┘          └──────────────┘          └──────────────┘
```

**Setup:**

```bash
# Terminal 1: Create SSH tunnel
ssh -i ~/.ssh/airflow_vm_key -L 8080:localhost:8080 azureuser@<VM_IP>

# Keep this terminal open! The tunnel stays active while connected.

# Terminal 2 (or browser): Access Airflow
open http://localhost:8080
# Login: admin / <AIRFLOW_PASSWORD>
```

**How it works:**
1. SSH connects to VM on port 22
2. `-L 8080:localhost:8080` forwards your local port 8080 to VM's localhost:8080
3. Airflow container binds to `127.0.0.1:8080` (localhost only, not exposed externally)
4. Your browser connects to `localhost:8080` → tunneled to VM → reaches Airflow

### Option 3: VS Code Remote SSH

```bash
# Install Remote-SSH extension, then:
code --remote ssh-remote+azureuser@<VM_IP> /opt/airflow
```

---

## VM Architecture

### Docker Containers on VM

```
┌─────────────────────────────────────────────────────────────────┐
│                     Airflow VM (Ubuntu 22.04)                   │
│                                                                 │
│  /opt/airflow/                                                  │
│  ├── docker-compose.yaml                                        │
│  ├── .env (credentials)                                         │
│  └── dags/                                                      │
│                                                                 │
│  ┌─────────────────────────────────────────────────────────────┐│
│  │                    Docker Compose Stack                     ││
│  │                                                             ││
│  │  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐         ││
│  │  │  PostgreSQL │  │  Webserver  │  │  Scheduler  │         ││
│  │  │   :5432     │  │   :8080     │  │             │         ││
│  │  │  (metadata) │  │   (UI)      │  │  (executor) │         ││
│  │  └─────────────┘  └──────┬──────┘  └─────────────┘         ││
│  │                          │                                  ││
│  │                   127.0.0.1:8080                            ││
│  │                   (localhost only)                          ││
│  └─────────────────────────────────────────────────────────────┘│
└─────────────────────────────────────────────────────────────────┘
```

### Container Details

| Container | Image | Purpose | Port |
|-----------|-------|---------|------|
| `postgres` | postgres:13 | Airflow metadata DB | 5432 (internal) |
| `airflow-webserver` | Custom | Web UI | 127.0.0.1:8080 |
| `airflow-scheduler` | Custom | DAG scheduling & task execution | - |
| `airflow-init` | Custom | One-time initialization | - |

### Key Files on VM

| Path | Purpose |
|------|---------|
| `/opt/airflow/` | Airflow root directory |
| `/opt/airflow/.env` | Environment variables (credentials) |
| `/opt/airflow/dags/` | DAG Python files |
| `/opt/airflow/logs/` | Task execution logs |
| `/opt/airflow/docker-compose.yaml` | Container orchestration |

---

## Data Lake Structure

### Container Layout (Medallion Architecture)

```
trinitylake (Storage Account)
│
├── bronze/                      # Raw data (immutable)
│   └── erp/
│       └── sales/
│           └── date=2025-01-15/
│               └── sales_raw.json
│
├── silver/                      # Cleaned & validated
│   └── erp/
│       └── sales/
│           └── date=2025-01-15/
│               └── sales_cleaned.csv
│
├── gold/                        # Business aggregates
│   ├── analytics/
│   │   └── sales_by_region/
│   │       └── date=2025-01-15/
│   │           └── aggregates.csv
│   └── serving/
│       └── daily_summary/
│           └── date=2025-01-15/
│               └── summary.json
│
└── synapse/                     # Synapse workspace storage
```

### Access Patterns

| Consumer | Access Method | Authentication |
|----------|--------------|----------------|
| Airflow DAGs | `azure.storage.filedatalake` SDK | Storage Account Key |
| VM Managed Identity | ABFS URLs | Azure RBAC |
| Synapse | External Data Source | SAS Token |
| Direct (CLI) | `az storage` commands | Account Key |

### ABFS URLs

```
Bronze: abfss://bronze@trinitylake.dfs.core.windows.net/
Silver: abfss://silver@trinitylake.dfs.core.windows.net/
Gold:   abfss://gold@trinitylake.dfs.core.windows.net/
```

---

## Authentication & Access

### Developer Access (You)

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                         How You Connect to Everything                        │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│                              ┌─────────┐                                    │
│                              │   You   │                                    │
│                              └────┬────┘                                    │
│           ┌────────────┬─────────┼─────────┬────────────┐                   │
│           │            │         │         │            │                   │
│           ▼            ▼         ▼         ▼            ▼                   │
│      ┌─────────┐ ┌──────────┐ ┌──────┐ ┌─────────┐ ┌─────────┐             │
│      │ Airflow │ │Data Lake │ │Synapse│ │Azure SQL│ │   VM    │             │
│      │   UI    │ │          │ │      │ │(optional)│ │  Shell  │             │
│      └────┬────┘ └────┬─────┘ └──┬───┘ └────┬────┘ └────┬────┘             │
│           │           │          │          │           │                   │
│       SSH Tunnel   az CLI    SQL Auth   SQL Auth    SSH Key                 │
│       + browser    or Portal  sqladmin  sqladmin                            │
│                    or SDK                                                   │
└─────────────────────────────────────────────────────────────────────────────┘
```

| Service | How to Connect | Tool |
|---------|---------------|------|
| **Airflow UI** | `ssh -L 8080:localhost:8080` then `http://localhost:8080` | Browser |
| **VM Shell** | `ssh -i ~/.ssh/airflow_vm_key azureuser@<IP>` | Terminal |
| **Data Lake** | `az storage fs file list --account-name <name> --file-system gold` | Azure CLI |
| **Data Lake** | Azure Portal → Storage Account → Containers | Browser |
| **Synapse** | Connect to `<workspace>-ondemand.sql.azuresynapse.net` | Azure Data Studio, Synapse Studio |
| **Azure SQL** | Connect to `<server>.database.windows.net` | Azure Data Studio, any SQL client |

### Pipeline Access (Airflow DAGs)

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                      How DAGs Connect to Services                            │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│                         ┌──────────────────┐                                │
│                         │   Airflow DAG    │                                │
│                         │  (runs on VM)    │                                │
│                         └────────┬─────────┘                                │
│                    ┌─────────────┼─────────────┐                            │
│                    │             │             │                            │
│                    ▼             ▼             ▼                            │
│             ┌───────────┐ ┌───────────┐ ┌ ─ ─ ─ ─ ─ ┐                       │
│             │ Data Lake │ │  Synapse  │   Azure SQL                         │
│             └─────┬─────┘ └───────────┘ │(optional) │                       │
│                   │                       ─ ─ ─ ─ ─ ─                        │
│                   │                            │                            │
│            Storage Key                   SQL Password                       │
│            (from .env)                   (from .env)                        │
│                                                                             │
│  Future: VM Managed Identity can replace storage key (RBAC already set up) │
└─────────────────────────────────────────────────────────────────────────────┘
```

### Quick Access Commands

```bash
# Airflow UI (via SSH tunnel)
ssh -i ~/.ssh/airflow_vm_key -L 8080:localhost:8080 azureuser@<VM_IP>
open http://localhost:8080

# VM Shell
ssh -i ~/.ssh/airflow_vm_key azureuser@<VM_IP>

# Data Lake (list files)
az storage fs file list --account-name <STORAGE> --file-system gold -o table

# Synapse (via Azure Data Studio or sqlcmd)
# Server: <workspace>-ondemand.sql.azuresynapse.net
# Database: trinity
# User: sqladmin
```

---

## Data Pipeline Flow

### DAG: `datalake_sample`

```
┌───────────────────────────────────────────────────────────────────────────────────┐
│                                                                                   │
│  Task 1                Task 2                Task 3               Task 4          │
│  ┌──────────┐         ┌──────────┐         ┌──────────┐        ┌ ─ ─ ─ ─ ─ ─ ┐   │
│  │ write_to │────────▶│transform │────────▶│aggregate │╌╌╌╌╌╌▶   load_to      │   │
│  │ _bronze  │         │_to_silver│         │ _to_gold │        │   _sql       │   │
│  └──────────┘         └──────────┘         └──────────┘         ─ ─ ─ ─ ─ ─ ─    │
│       │                    │                    │                     :           │
│       ▼                    ▼                    ▼                     ▼           │
│  ┌──────────┐         ┌──────────┐         ┌──────────┐        ┌ ─ ─ ─ ─ ─ ─ ┐   │
│  │  Bronze  │         │  Silver  │         │   Gold   │          Azure SQL    │   │
│  │   JSON   │         │   CSV    │         │   JSON   │        │  (optional)  │   │
│  └──────────┘         └──────────┘         └──────────┘         ─ ─ ─ ─ ─ ─ ─    │
│                                                                                   │
│  ─────────── Core Pipeline ───────────────────────────  ╌╌╌╌ Optional ╌╌╌╌╌╌╌╌   │
└───────────────────────────────────────────────────────────────────────────────────┘
```

| Task | Input | Output | Required |
|------|-------|--------|----------|
| `write_to_bronze` | Generated data | `bronze/erp/sales/.../sales_raw.json` | Yes |
| `transform_to_silver` | Bronze JSON | `silver/erp/sales/.../sales_cleaned.csv` | Yes |
| `aggregate_to_gold` | Silver CSV | `gold/serving/.../summary.json` | Yes |
| `load_to_sql` | Gold JSON | `sales_by_region`, `sales_by_product` tables | **Optional** - skips if SQL not configured |

---

## Resources Summary

| Resource | Name | Purpose | Cost/mo (CAD) |
|----------|------|---------|---------------|
| VM | `airflow-vm` | Airflow orchestration (D4s_v3) | ~$190 |
| Storage | `trinitylake` | Data Lake (ADLS Gen2) | ~$7-30 |
| Synapse | `trinitysynapse` | Ad-hoc SQL queries + Power BI | ~$7-70 |
| **Core Total** | | | **~$230-320** |
| | | | |
| Azure SQL | `trinitydb` | *(optional)* Fast DirectQuery | +~$20 |

---

## Deployment

### Prerequisites

| Requirement | Status |
|-------------|--------|
| Azure Subscription | F12 (`3bd909b3-7362-4c30-8ed8-0bc6b2f3929c`) |
| Permission | Subscription Contributor ✅ |
| Azure CLI | `brew install azure-cli` |
| Python 3.8+ | For Synapse setup script |

### Phase 1: Register Providers (~2 min)

```bash
az account set --subscription "3bd909b3-7362-4c30-8ed8-0bc6b2f3929c"

az provider register --namespace Microsoft.Compute --wait
az provider register --namespace Microsoft.Network --wait
az provider register --namespace Microsoft.Storage --wait
az provider register --namespace Microsoft.Synapse --wait
az provider register --namespace Microsoft.Sql --wait
```

### Phase 2: Create Infrastructure (~12 min)

```bash
cd ~/code/gaon/trinity/infra
chmod +x *.sh

# Set variables
export SUBSCRIPTION_ID="3bd909b3-7362-4c30-8ed8-0bc6b2f3929c"
export RESOURCE_GROUP="trinity-rg"
export LOCATION="eastus2"
export VM_NAME="airflow-vm"
export VNET_NAME="trinity-vnet"
export SUBNET_NAME="trinity-subnet"

# Core infrastructure (required)
./01-create-vm.sh           # ~5 min → Save: VM_IP, SSH_KEY
./03-create-datalake.sh     # ~2 min → Save: STORAGE_NAME, STORAGE_KEY
./04-create-synapse.sh      # ~5 min → Save: SYNAPSE_ENDPOINT, SYNAPSE_PASSWORD

# Optional: Add later if needed for fast Power BI DirectQuery
# ./05-create-azure-sql.sh  # ~3 min → Save: SQL_SERVER, SQL_PASSWORD
```

### Phase 3: Setup VM (~10 min)

```bash
# Copy files
scp -i ~/.ssh/airflow_vm_key -r ../airflow 02-setup-vm.sh azureuser@<VM_IP>:~/

# SSH and setup
ssh -i ~/.ssh/airflow_vm_key azureuser@<VM_IP>
chmod +x 02-setup-vm.sh && ./02-setup-vm.sh  # Save: AIRFLOW_PASSWORD

# Configure credentials (Data Lake only - SQL is optional)
cat >> /opt/airflow/.env << 'EOF'
AZURE_STORAGE_ACCOUNT_NAME=<STORAGE_NAME>
AZURE_STORAGE_ACCOUNT_KEY=<STORAGE_KEY>
EOF

# Start Airflow (MUST use down/up, not restart!)
cd /opt/airflow
docker compose build
docker compose up -d
```

### Adding Azure SQL Later (Optional)

If you need faster Power BI dashboards or scan costs grow:

```bash
# 1. Create Azure SQL
./05-create-azure-sql.sh    # Save: SQL_SERVER, SQL_PASSWORD

# 2. Add credentials to VM
ssh -i ~/.ssh/airflow_vm_key azureuser@<VM_IP>
cat >> /opt/airflow/.env << 'EOF'
AZURE_SQL_SERVER=<SQL_SERVER>.database.windows.net
AZURE_SQL_DATABASE=trinity
AZURE_SQL_USER=sqladmin
AZURE_SQL_PASSWORD=<SQL_PASSWORD>
EOF

# 3. Restart Airflow to pick up new credentials
cd /opt/airflow && docker compose down && docker compose up -d
```

### Phase 4: Configure Synapse (~2 min)

```bash
# From local machine
cd ~/code/gaon/trinity

export SYNAPSE_SERVER="<SYNAPSE_ENDPOINT>"
export SYNAPSE_USER="sqladmin"
export SYNAPSE_PASSWORD="<SYNAPSE_PASSWORD>"
export STORAGE_ACCOUNT_NAME="<STORAGE_NAME>"
export STORAGE_ACCOUNT_KEY="<STORAGE_KEY>"

python3 scripts/synapse_setup.py
```

### Phase 5: Verify

```bash
# Access Airflow
open http://<VM_IP>:8080  # Login: admin / <AIRFLOW_PASSWORD>

# Or via SSH tunnel (more secure)
ssh -i ~/.ssh/airflow_vm_key -L 8080:localhost:8080 azureuser@<VM_IP>
open http://localhost:8080
```

**Test:** Enable and trigger `datalake_sample` DAG → first 3 tasks complete green (4th skips if no SQL configured).

---

## Credentials Template

After deployment, save these securely:

```yaml
# VM
vm_ip: "<from 01-create-vm.sh>"
ssh_key: "~/.ssh/airflow_vm_key"
airflow_url: "http://<VM_IP>:8080"
airflow_user: "admin"
airflow_password: "<from 02-setup-vm.sh>"

# Data Lake
storage_account: "<from 03-create-datalake.sh>"
storage_key: "<from 03-create-datalake.sh>"

# Synapse
synapse_endpoint: "<workspace>-ondemand.sql.azuresynapse.net"
synapse_database: "trinity"
synapse_user: "sqladmin"
synapse_password: "<from 04-create-synapse.sh>"

# Azure SQL (optional - add when needed)
# sql_server: "<server>.database.windows.net"
# sql_database: "trinity"
# sql_user: "sqladmin"
# sql_password: "<from 05-create-azure-sql.sh>"
```

---

## Operations

### Quick Commands

| Task | Command |
|------|---------|
| SSH to VM | `ssh -i ~/.ssh/airflow_vm_key azureuser@<VM_IP>` |
| Airflow status | `docker compose ps` (on VM) |
| Trigger DAG | `docker compose exec airflow-scheduler airflow dags trigger datalake_sample` |
| View logs | `docker compose logs airflow-scheduler --tail 50` |
| Restart Airflow | `docker compose down && docker compose up -d` |

### Synapse Queries

**Important:** Always use `trinity` database (not `master`).

```sql
-- Query Gold layer
SELECT *
FROM OPENROWSET(
    BULK 'gold/serving/daily_summary/*/summary.json',
    DATA_SOURCE = 'TrinityLake',
    FORMAT = 'CSV',
    PARSER_VERSION = '2.0',
    FIELDTERMINATOR = '0x0b',
    FIELDQUOTE = '0x0b'
) WITH (doc NVARCHAR(MAX)) AS rows;

-- Query Silver layer CSV
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
    quantity VARCHAR(50),
    region VARCHAR(50)
) AS sales;
```

### Power BI Connection

| Setting | Value |
|---------|-------|
| Server | `<server>.database.windows.net` |
| Database | `trinity` |
| Authentication | SQL Server |
| Username | `sqladmin` |
| Tables | `sales_by_region`, `sales_by_product` |

---

## Troubleshooting

### Common Issues

| Symptom | Cause | Solution |
|---------|-------|----------|
| Provider not registered | First-time Azure use | `az provider register --namespace <name> --wait` |
| Env vars not loading | Used `restart` | Use `docker compose down && up -d` |
| Synapse credential error | Wrong database | Switch to `trinity` (not `master`) |
| `**` wildcard fails | Synapse limitation | Use single `*` instead |
| SQL type conversion error | CSV parsing | Use `VARCHAR` for all, cast in SELECT |
| Storage name taken | Globally unique | Script auto-adds random suffix |

### Synapse-Specific

```sql
-- Check credentials exist
SELECT * FROM sys.database_scoped_credentials;

-- Check data sources exist
SELECT * FROM sys.external_data_sources;

-- Must be in 'trinity' database, not 'master'!
```

### Azure SQL-Specific

```bash
# Check firewall allows your IP
az sql server firewall-rule list --server <server> --resource-group trinity-rg -o table

# Add your IP
MY_IP=$(curl -s ifconfig.me)
az sql server firewall-rule create --server <server> --resource-group trinity-rg \
    --name MyIP --start-ip-address $MY_IP --end-ip-address $MY_IP
```

---

## Cleanup

```bash
# Delete everything (irreversible!)
az group delete --name trinity-rg --yes --no-wait
```

---

## File Structure

```
trinity/
├── infra/
│   ├── variables.sh           # Configuration
│   ├── 01-create-vm.sh        # VM + networking
│   ├── 02-setup-vm.sh         # Docker + Airflow install
│   ├── 03-create-datalake.sh  # ADLS Gen2 + Private Endpoint
│   ├── 04-create-synapse.sh   # Synapse workspace
│   └── 05-create-azure-sql.sh # Azure SQL Database
├── airflow/
│   ├── Dockerfile             # Custom image with pymssql
│   ├── docker-compose.yaml    # Airflow services
│   ├── .env.example           # Environment template
│   └── dags/
│       └── datalake_sample_dag.py  # Bronze → Silver → Gold → SQL
├── scripts/
│   └── synapse_setup.py       # Synapse credential setup
└── .github/workflows/
    └── deploy-airflow.yml     # CI/CD pipeline
```

---

## CI/CD (Optional)

### GitHub Secrets

| Secret | Value |
|--------|-------|
| `AIRFLOW_VM_IP` | VM public IP |
| `AIRFLOW_SSH_USER` | `azureuser` |
| `AIRFLOW_SSH_KEY` | Contents of `~/.ssh/airflow_vm_key` |

### Test Locally

```bash
brew install act
act -j validate --container-architecture linux/amd64
```

---

## Cost Breakdown

| Resource | SKU | Monthly (CAD) | Notes |
|----------|-----|---------------|-------|
| VM | D4s_v3 | ~$190 | 4 vCPU, 16GB RAM |
| VM Disk | 128GB Premium SSD | ~$27 | |
| Public IP | Static | ~$5 | |
| Data Lake | Standard_LRS | ~$7-30 | Pay per GB + operations |
| Synapse | Serverless | ~$7-70 | $7/TB scanned |
| Azure SQL | S0 (10 DTU) | ~$20 | |

**Cost Saving Options:**
- Reserved VM (1-year): ~40% off
- Smaller VM (D2s_v3): ~$95/mo
- Stop VM nights/weekends
- SQL Basic tier for dev: ~$5/mo

---

## Security

| Layer | Configuration |
|-------|---------------|
| VM Access | SSH key only (port 22) |
| Airflow UI | SSH tunnel (port 8080 not exposed) |
| Data Lake | Private Endpoint + VM Managed Identity |
| Synapse | Private Endpoint + Azure services firewall |
| Azure SQL | Private Endpoint + Azure services firewall |
| Authentication | VM uses Managed Identity for storage access |
