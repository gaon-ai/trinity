FROM apache/airflow:2.8.1

USER root
RUN apt-get update && apt-get install -y --no-install-recommends \
    freetds-dev \
    build-essential \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

USER airflow

# Install Azure and data processing packages
# Use pymssql 2.2.x which has fewer build issues
RUN pip install --no-cache-dir \
    azure-storage-file-datalake \
    pandas \
    pyarrow \
    "pymssql>=2.2.0,<2.3.0"
