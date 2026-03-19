FROM python:3.10-slim

RUN apt-get update && apt-get install -y git && rm -rf /var/lib/apt/lists/*

RUN pip install --upgrade pip && \
    pip install \
    numpy==1.24.4 \
    pandas==1.5.3 \
    pyarrow==11.0.0 \
    thrift==0.16.0 \
    databricks-sdk==0.9.0 \
    dbt-core==1.6.0 \
    dbt-databricks==1.6.0 \
    dbt-spark==1.6.0

WORKDIR /usr/app/dbt

ENTRYPOINT ["tail", "-f", "/dev/null"]