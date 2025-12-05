# Infrastructure Request: Airflow VM for Data Lakehouse

## Overview

We need an Azure VM to run Apache Airflow for orchestrating data pipelines in our lakehouse architecture.

**Purpose:** ETL/ELT orchestration for Bronze → Silver → Gold data transformations

---

## VM Specifications

| Requirement | Value |
|-------------|-------|
| Cloud Provider | Azure |
| Region | `eastus2` |
| VM Size | `Standard_D4s_v3` (4 vCPU, 16 GB RAM) |
| OS | Ubuntu 22.04 LTS |
| Disk | 128 GB Premium SSD |
| Public IP | Static |

---

## Network / Firewall Rules

| Port | Protocol | Source | Purpose |
|------|----------|--------|---------|
| 22 | TCP | VPN range or specific IPs | SSH access |
| 8080 | TCP | VPN range or all* | Airflow Web UI |

*Note: For development/testing, we may need port 8080 open to all IPs temporarily.

---

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

---

## Software Requirements

Please pre-install:

- Docker CE (latest)
- Docker Compose v2

Or grant sudo access and we will install ourselves.

---

## CI/CD Access

For automated deployments, please generate a dedicated SSH key pair:

| Item | Details |
|------|---------|
| Key name | `airflow-cicd` |
| Access | Read/write to `/opt/airflow/dags/` |

Provide us (via secure channel):
- Private key
- VM IP address

---

## Directory Structure

Please create this directory on the VM with appropriate permissions:

```bash
sudo mkdir -p /opt/airflow/{dags,logs,plugins,config}
sudo chown -R azureuser:azureuser /opt/airflow
```

---

## Additional: Data Lake Storage (ADLS Gen2)

We also need Azure Data Lake Storage Gen2 for our lakehouse:

| Requirement | Value |
|-------------|-------|
| Storage Type | ADLS Gen2 (hierarchical namespace enabled) |
| SKU | Standard_LRS |
| Containers | `bronze`, `silver`, `gold` |
| Access | Storage account key or managed identity |

---

## Deliverables

Please provide:

1. VM public IP address
2. Confirmation of firewall rules
3. SSH key or credentials (via secure channel)
4. Storage account name and key (if creating ADLS)

---

## Estimated Monthly Cost

| Resource | Cost |
|----------|------|
| VM (Standard_D4s_v3) | ~$140 |
| Storage (128 GB Premium SSD) | ~$20 |
| Data Lake (ADLS Gen2, pay per use) | ~$5-20 |
| Static Public IP | ~$3 |
| **Total** | **~$170** |

---

## Common Issues to Avoid

Based on our experience, please ensure:

1. **Azure providers are registered:**
   ```bash
   az provider register --namespace Microsoft.Storage
   az provider register --namespace Microsoft.Network
   az provider register --namespace Microsoft.Compute
   ```

2. **Storage account names are globally unique** (lowercase, no hyphens, 3-24 chars)

3. **Firewall rules allow access** - if IP-restricted, ensure our IPs are whitelisted

---

## Contact

For questions, contact: [YOUR_NAME / YOUR_EMAIL]

---

## Timeline

Requested by: [DATE]
