# Infrastructure Request: Data Lakehouse Platform

## Overview

We need Azure infrastructure to run a data lakehouse platform with the following components:

1. **Airflow VM** - Orchestration for data pipelines
2. **Data Lake (ADLS Gen2)** - Storage with Bronze/Silver/Gold layers
3. **Synapse Analytics** - Ad-hoc SQL queries on data lake

**Purpose:** ETL/ELT orchestration for Bronze → Silver → Gold data transformations with self-service analytics.

---

## Architecture Diagram

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
│                                                ┌─────────────────┐         │
│                                                │     Synapse     │         │
│                                                │   (Serverless)  │◀── SQL  │
│                                                └─────────────────┘         │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

# Component 1: Airflow VM

## VM Specifications

| Requirement | Value |
|-------------|-------|
| Cloud Provider | Azure |
| Resource Group | `airflow-rg` (or existing) |
| Region | `eastus2` |
| VM Size | `Standard_D4s_v3` (4 vCPU, 16 GB RAM) |
| OS | Ubuntu 22.04 LTS |
| Disk | 128 GB Premium SSD |
| Public IP | Static |

## Network / Firewall Rules

| Port | Protocol | Source | Purpose |
|------|----------|--------|---------|
| 22 | TCP | VPN range or specific IPs | SSH access |
| 8080 | TCP | VPN range or all* | Airflow Web UI |

*Note: For development/testing, we may need port 8080 open to all IPs temporarily.

## User Access

| Item | Value |
|------|-------|
| Username | `azureuser` |
| Auth method | SSH public key |
| Permissions | sudo access, docker group membership |

**SSH Public Key** (to be added to VM):
```
<PASTE_PUBLIC_KEY_HERE>
```

## Software Requirements

Please pre-install:

- Docker CE (latest)
- Docker Compose v2

Or grant sudo access and we will install ourselves.

## CI/CD Access

For automated deployments via GitHub Actions:

| Item | Details |
|------|---------|
| Key name | `airflow-cicd` |
| Access | Read/write to `/opt/airflow/dags/` |

Provide us (via secure channel):
- Private key
- VM IP address

## Directory Structure

Please create this directory on the VM with appropriate permissions:

```bash
sudo mkdir -p /opt/airflow/{dags,logs,plugins,config}
sudo chown -R azureuser:azureuser /opt/airflow
```

---

# Component 2: Data Lake (ADLS Gen2)

## Storage Account Specifications

| Requirement | Value |
|-------------|-------|
| Storage Type | Azure Data Lake Storage Gen2 |
| Hierarchical Namespace | **Enabled** (required) |
| SKU | Standard_LRS |
| Region | `eastus2` (same as VM) |
| Name | Globally unique, lowercase, 3-24 chars, no hyphens |

## Containers (Filesystems)

Create these three containers:

| Container | Purpose |
|-----------|---------|
| `bronze` | Raw data ingestion |
| `silver` | Cleaned/transformed data |
| `gold` | Aggregated/analytics-ready data |

## Access Requirements

| Access Type | Details |
|-------------|---------|
| Airflow VM | Storage account key or managed identity |
| Synapse | Managed identity with Storage Blob Data Reader role |
| Data Team | Storage account key for manual access |

## ACL Configuration

For Synapse to read files, ensure ACLs allow read access:

```bash
# Set on each container recursively
az storage fs access set-recursive \
  --acl "user::rwx,group::r-x,other::r-x" \
  --path "/" \
  --file-system bronze \
  --account-name <storage_account>
```

---

# Component 3: Synapse Analytics

## Workspace Specifications

| Requirement | Value |
|-------------|-------|
| Workspace Name | Globally unique (e.g., `companyname-synapse`) |
| Region | `eastus2` (same as storage) |
| Default Storage | Link to the ADLS Gen2 account above |
| SQL Admin | `sqladmin` |
| SQL Pool Type | **Serverless only** (no dedicated pool needed) |

## Firewall Rules

| Rule | IP Range | Purpose |
|------|----------|---------|
| AllowAllWindowsAzureIps | 0.0.0.0 - 0.0.0.0 | Azure services |
| AllowVPN | VPN IP range | Internal access |
| AllowAll* | 0.0.0.0 - 255.255.255.255 | Development (temporary) |

*Note: For production, restrict to VPN/specific IPs only.

## Role Assignments

Grant Synapse managed identity access to Data Lake:

| Principal | Role | Scope |
|-----------|------|-------|
| Synapse Managed Identity | Storage Blob Data Reader | Storage account |
| Synapse Managed Identity | Storage Blob Data Contributor | Storage account (if write needed) |

```bash
# Get Synapse managed identity
SYNAPSE_IDENTITY=$(az synapse workspace show --name <workspace> --resource-group <rg> --query "identity.principalId" -o tsv)

# Assign role
az role assignment create \
  --assignee "$SYNAPSE_IDENTITY" \
  --role "Storage Blob Data Reader" \
  --scope "/subscriptions/<sub>/resourceGroups/<rg>/providers/Microsoft.Storage/storageAccounts/<storage>"
```

---

# Azure Provider Registration

Ensure these providers are registered before creating resources:

```bash
az provider register --namespace Microsoft.Compute --wait
az provider register --namespace Microsoft.Network --wait
az provider register --namespace Microsoft.Storage --wait
az provider register --namespace Microsoft.Synapse --wait
az provider register --namespace Microsoft.Sql --wait
```

---

# Deliverables

Please provide via secure channel:

## Airflow VM
- [ ] VM public IP address
- [ ] SSH private key (for CI/CD)
- [ ] Confirmation of firewall rules

## Data Lake
- [ ] Storage account name
- [ ] Storage account key
- [ ] Confirmation of containers created (bronze, silver, gold)
- [ ] Confirmation of hierarchical namespace enabled

## Synapse
- [ ] Workspace name
- [ ] SQL endpoint (serverless): `<workspace>-ondemand.sql.azuresynapse.net`
- [ ] SQL admin username and password
- [ ] Synapse Studio URL
- [ ] Confirmation of role assignments to storage

---

# Estimated Monthly Cost

| Resource | Cost |
|----------|------|
| VM (Standard_D4s_v3) | ~$140 |
| VM Storage (128 GB Premium SSD) | ~$20 |
| Static Public IP | ~$3 |
| Data Lake (ADLS Gen2, pay per use) | ~$5-20 |
| Synapse Serverless (pay per TB scanned) | ~$5-50 |
| **Total** | **~$175-235** |

Note: Synapse serverless has no idle cost - you only pay when queries run (~$5/TB scanned).

---

# Common Issues to Avoid

Based on our experience, please ensure:

1. **Azure providers are registered** (see above)

2. **Storage account has hierarchical namespace enabled**
   - Must be set at creation time, cannot be changed later
   - Required for ADLS Gen2 features

3. **Storage account names are globally unique**
   - Lowercase only, no hyphens
   - 3-24 characters

4. **Synapse credentials in user database, not master**
   - Create a database (e.g., `analytics`) for credentials
   - `master` database doesn't allow scoped credentials

5. **ACLs allow read access for Synapse**
   - RBAC alone may not be sufficient
   - Set `other::r-x` on directories and files

6. **Firewall rules allow access**
   - If IP-restricted, whitelist our IPs
   - For Synapse, allow Azure services

---

# Self-Service Scripts

If you prefer, we can create the resources ourselves using our provided scripts:

```bash
# Clone repository
git clone https://github.com/gaon-ai/trinity.git
cd trinity/infra

# Set subscription
export SUBSCRIPTION_ID="<your-subscription-id>"

# Create all resources
./01-create-vm.sh        # Creates VM
./03-create-datalake.sh  # Creates ADLS Gen2
./04-create-synapse.sh   # Creates Synapse

# Configure Synapse credentials
python3 ../scripts/synapse_setup.py
```

We just need:
- Subscription ID with Contributor access
- Resource group name (or permission to create one)

---

# Contact

For questions, contact: [YOUR_NAME / YOUR_EMAIL]

---

# Timeline

Requested by: [DATE]

Priority: [HIGH / MEDIUM / LOW]
