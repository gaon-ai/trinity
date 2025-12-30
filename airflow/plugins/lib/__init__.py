"""
Shared DAG Utilities

Usage in DAGs:
    from lib.datalake import write_to_datalake, read_from_datalake
    from lib.utils import get_date_hour, build_bronze_path
    from lib.datasets import BRONZE_SALES, SILVER_SALES
"""
from lib.datalake import (
    get_datalake_client,
    write_to_datalake,
    read_from_datalake,
    file_exists,
    wait_for_file,
    wait_for_files,
    write_metadata,
)
from lib.utils import (
    get_date_hour,
    get_execution_date,
    build_bronze_path,
    build_silver_path,
    build_gold_path,
)
from lib.datasets import (
    BRONZE_SALES,
    SILVER_SALES,
    GOLD_SALES_BY_REGION,
    GOLD_SALES_BY_PRODUCT,
    GOLD_DAILY_SUMMARY,
)
