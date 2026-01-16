"""
Client SQL Server table configurations.

Tables from the client's on-prem SQL Server database (Prod_Trinity_App),
accessible via site-to-site VPN.

Tables are organized by ingestion pattern:
- incremental.py: Timestamp-based incremental sync (daily)
- full_refresh.py: Full table refresh (weekly/daily)
"""
from lib.tables.client.incremental import INCREMENTAL_TABLES
from lib.tables.client.full_refresh import FULL_REFRESH_TABLES

# Combined list of all client tables
ALL_TABLES = INCREMENTAL_TABLES + FULL_REFRESH_TABLES

__all__ = [
    "INCREMENTAL_TABLES",
    "FULL_REFRESH_TABLES",
    "ALL_TABLES",
]
