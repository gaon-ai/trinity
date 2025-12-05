# Azure Synapse Serverless SQL Setup Guide

This guide documents the setup and troubleshooting for querying Azure Data Lake Storage (ADLS Gen2) from Synapse Serverless SQL.

## Prerequisites

- Azure Synapse workspace created
- ADLS Gen2 storage account with data
- Storage account key or SAS token

## Quick Start

### 1. Create the Synapse Workspace

```bash
cd infra
export SUBSCRIPTION_ID="your-subscription-id"
export STORAGE_ACCOUNT_NAME="your-storage-account"
./04-create-synapse.sh
```

### 2. Run Setup Script

```bash
# Set your values
export SYNAPSE_SERVER="your-workspace-ondemand.sql.azuresynapse.net"
export SYNAPSE_USER="sqladmin"
export SYNAPSE_PASSWORD="your-password"
export STORAGE_ACCOUNT_NAME="your-storage-account"
export STORAGE_ACCOUNT_KEY="your-storage-key"

# Run setup
python3 scripts/synapse_setup.py
```

### 3. Query Your Data

In Synapse Studio:
1. **Switch to `trinity` database** (dropdown at top - NOT `master`)
2. Run your query using `DATA_SOURCE = 'TrinityLake'`

---

## Important: Common Pitfalls

### ❌ Don't Use `master` Database

Credentials and data sources **cannot** be created in the `master` database.

```sql
-- This will FAIL in master database:
CREATE DATABASE SCOPED CREDENTIAL MyCredential ...
-- Error: "CREATE DATABASE SCOPED CREDENTIAL is not supported in master database"
```

**Solution:** Always create and use a dedicated database:
```sql
CREATE DATABASE trinity;
-- Then switch to 'trinity' database before creating credentials
```

### ❌ Don't Rely on Managed Identity (Initially)

Managed identity permissions can take 10-30 minutes to propagate and require:
- Correct RBAC role assignments
- Correct ACLs on files (`other::r-x` not `other::---`)
- Time for propagation

**Solution:** Use SAS token authentication for reliable access:
```sql
CREATE DATABASE SCOPED CREDENTIAL TrinityStorageKey
WITH IDENTITY = 'SHARED ACCESS SIGNATURE',
SECRET = 'your-sas-token';
```

### ❌ Don't Use `**` Wildcards

Synapse doesn't support recursive glob patterns.

```sql
-- This will FAIL:
BULK 'silver/erp/sales/**/*.csv'
-- Error: "Consecutive wildcard characters present in path"
```

**Solution:** Use single `*` or explicit paths:
```sql
-- Use single wildcard
BULK 'silver/erp/sales/*/*.csv'

-- Or explicit path
BULK 'silver/erp/sales/date=2025-12-05/sales_cleaned.csv'
```

### ❌ Don't Use `HEADER_ROW = TRUE` with Parser 2.0

Can cause schema detection issues.

**Solution:** Use `FIRSTROW = 2` instead:
```sql
SELECT *
FROM OPENROWSET(
    BULK 'path/to/file.csv',
    DATA_SOURCE = 'TrinityLake',
    FORMAT = 'CSV',
    PARSER_VERSION = '2.0',
    FIRSTROW = 2  -- Skip header row
) WITH (
    ...
) AS data
```

### ❌ Don't Use Specific Types Initially

Type conversion errors are common with CSV files.

```sql
-- This may FAIL:
WITH (order_id INT, ...)
-- Error: "Bulk load data conversion error"
```

**Solution:** Start with `VARCHAR` for everything, then cast:
```sql
SELECT
    CAST(order_id AS INT) as order_id,
    CAST(unit_price AS DECIMAL(10,2)) as unit_price,
    ...
FROM OPENROWSET(...) WITH (
    order_id VARCHAR(50),
    unit_price VARCHAR(50),
    ...
) AS data
```

---

## Complete Working Example

### Step 1: Create Database (run in `master`)

```sql
CREATE DATABASE trinity;
```

### Step 2: Create Credentials (run in `trinity` database)

```sql
-- Create master key
CREATE MASTER KEY ENCRYPTION BY PASSWORD = 'YourSecurePassword123!';

-- Create credential with SAS token
CREATE DATABASE SCOPED CREDENTIAL TrinityStorageKey
WITH IDENTITY = 'SHARED ACCESS SIGNATURE',
SECRET = 'se=2026-12-31...your-sas-token...';

-- Create external data source
CREATE EXTERNAL DATA SOURCE TrinityLake
WITH (
    LOCATION = 'https://gaaborotrinity.dfs.core.windows.net',
    CREDENTIAL = TrinityStorageKey
);
```

### Step 3: Query Data

```sql
-- Query Silver layer
SELECT *
FROM OPENROWSET(
    BULK 'silver/erp/sales/date=2025-12-05/sales_cleaned.csv',
    DATA_SOURCE = 'TrinityLake',
    FORMAT = 'CSV',
    PARSER_VERSION = '2.0',
    FIRSTROW = 2
) WITH (
    order_id VARCHAR(50),
    customer_id VARCHAR(50),
    product VARCHAR(100),
    quantity VARCHAR(50),
    unit_price VARCHAR(50),
    order_date VARCHAR(50),
    region VARCHAR(50),
    total_amount VARCHAR(50),
    processed_at VARCHAR(100)
) AS sales;

-- Query with wildcards (single level)
SELECT *
FROM OPENROWSET(
    BULK 'silver/erp/sales/*/sales_cleaned.csv',
    DATA_SOURCE = 'TrinityLake',
    FORMAT = 'CSV',
    PARSER_VERSION = '2.0',
    FIRSTROW = 2
) WITH (
    order_id VARCHAR(50),
    customer_id VARCHAR(50),
    product VARCHAR(100),
    quantity VARCHAR(50),
    unit_price VARCHAR(50),
    order_date VARCHAR(50),
    region VARCHAR(50),
    total_amount VARCHAR(50),
    processed_at VARCHAR(100)
) AS sales;

-- Query JSON files
SELECT
    JSON_VALUE(doc, '$.report_date') AS report_date,
    JSON_VALUE(doc, '$.total_orders') AS total_orders,
    JSON_VALUE(doc, '$.total_revenue') AS total_revenue
FROM OPENROWSET(
    BULK 'gold/serving/daily_summary/*/summary.json',
    DATA_SOURCE = 'TrinityLake',
    FORMAT = 'CSV',
    PARSER_VERSION = '2.0',
    FIELDTERMINATOR = '0x0b',
    FIELDQUOTE = '0x0b'
) WITH (doc NVARCHAR(MAX)) AS rows;
```

---

## Generate SAS Token

```bash
# Generate SAS token valid until 2026
az storage account generate-sas \
  --account-name gaaborotrinity \
  --account-key "YOUR_ACCOUNT_KEY" \
  --services bf \
  --resource-types sco \
  --permissions rl \
  --expiry 2026-12-31T00:00:00Z \
  --https-only \
  -o tsv
```

---

## Troubleshooting

### "File cannot be opened because it does not exist"

1. Verify file exists:
   ```bash
   az storage blob list --account-name gaaborotrinity --container-name silver -o table
   ```

2. Check you're in the right database (not `master`)

3. Verify credential and data source exist:
   ```sql
   SELECT * FROM sys.database_scoped_credentials;
   SELECT * FROM sys.external_data_sources;
   ```

4. Check SAS token hasn't expired

### "Bulk load data conversion error"

Use `VARCHAR` for all columns in the `WITH` clause, then cast in SELECT.

### "CREATE DATABASE SCOPED CREDENTIAL is not supported"

You're in the `master` database. Switch to a user database.

### "Consecutive wildcard characters"

Replace `**` with single `*` or use explicit paths.

---

## Current Configuration

| Setting | Value |
|---------|-------|
| Synapse Workspace | `trinitysynapse` |
| SQL Endpoint | `trinitysynapse-ondemand.sql.azuresynapse.net` |
| Database | `trinity` |
| SQL Admin | `sqladmin` |
| Data Source | `TrinityLake` |
| Credential | `TrinityStorageKey` |
| Storage Account | `gaaborotrinity` |

---

## Cost

Synapse Serverless SQL charges per TB of data scanned:
- ~$5 per TB scanned
- No charge when idle
- Minimum charge per query: small fraction of a cent
