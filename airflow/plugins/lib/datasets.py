"""
Airflow Dataset Definitions

Central location for all dataset URIs used in data lineage.
These datasets connect DAGs and track data dependencies.
"""
from airflow import Dataset

# Bronze layer datasets (raw data)
BRONZE_SALES = Dataset("azure://trinitylake/bronze/erp/sales")
BRONZE_CLIENT_DATA = Dataset("azure://trinitylake/bronze/client")

# Silver layer datasets (cleaned data)
SILVER_SALES = Dataset("azure://trinitylake/silver/sales")

# Gold layer datasets - Sales aggregates
GOLD_SALES_BY_REGION = Dataset("azure://trinitylake/gold/sales_by_region")
GOLD_SALES_BY_PRODUCT = Dataset("azure://trinitylake/gold/sales_by_product")
GOLD_DAILY_SUMMARY = Dataset("azure://trinitylake/gold/daily_summary")

# Gold layer datasets - Client facts and dimensions
GOLD_FACT_INVOICE = Dataset("azure://trinitylake/gold/fact_invoice")
GOLD_FACT_INVOICE_DETAIL = Dataset("azure://trinitylake/gold/fact_invoice_detail")
GOLD_FACT_ORDER_ITEM = Dataset("azure://trinitylake/gold/fact_order_item")
GOLD_DIM_CUSTOMER = Dataset("azure://trinitylake/gold/dim_customer")
GOLD_MARGIN_INVOICE = Dataset("azure://trinitylake/gold/margin_invoice")
GOLD_FACT_GENERAL_LEDGER = Dataset("azure://trinitylake/gold/fact_general_ledger")
GOLD_MARGIN_REBATE = Dataset("azure://trinitylake/gold/margin_rebate")
