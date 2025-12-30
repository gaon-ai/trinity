"""
Airflow Dataset Definitions

Central location for all dataset URIs used in data lineage.
These datasets connect DAGs and track data dependencies.
"""
from airflow import Dataset

# Bronze layer datasets (raw data)
BRONZE_SALES = Dataset("azure://trinitylake/bronze/erp/sales")

# Silver layer datasets (cleaned data)
SILVER_SALES = Dataset("azure://trinitylake/silver/sales")

# Gold layer datasets (aggregates)
GOLD_SALES_BY_REGION = Dataset("azure://trinitylake/gold/sales_by_region")
GOLD_SALES_BY_PRODUCT = Dataset("azure://trinitylake/gold/sales_by_product")
GOLD_DAILY_SUMMARY = Dataset("azure://trinitylake/gold/daily_summary")
