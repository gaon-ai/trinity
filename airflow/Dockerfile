FROM apache/airflow:2.8.1

USER root
RUN apt-get update && apt-get install -y --no-install-recommends \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

USER airflow

# Install Azure and data processing packages
RUN pip install --no-cache-dir \
    azure-storage-file-datalake \
    pandas \
    pyarrow
