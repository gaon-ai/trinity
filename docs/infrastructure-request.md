# Infrastructure Request: Airflow VM

## Overview

We need an Azure VM to run Apache Airflow for workflow orchestration.

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
| 22 | TCP | VPN range only | SSH access |
| 8080 | TCP | VPN range only | Airflow Web UI |

---

## User Access

| Item | Value |
|------|-------|
| Username | `azureuser` |
| Auth method | SSH public key |
| Permissions | sudo access, docker group |

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

## Deliverables

Please provide:

1. VM public IP address
2. Confirmation of firewall rules
3. SSH key or Service Principal credentials (via secure channel)

---

## Estimated Monthly Cost

| Resource | Cost |
|----------|------|
| VM (Standard_D4s_v3) | ~$140 |
| Storage (128 GB Premium SSD) | ~$20 |
| Static Public IP | ~$3 |
| **Total** | **~$163** |

---

## Contact

For questions, contact: [YOUR_NAME / YOUR_EMAIL]

---

## Timeline

Requested by: [DATE]
