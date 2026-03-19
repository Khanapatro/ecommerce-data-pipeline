FROM python:3.10-slim

RUN pip install --upgrade pip && \
    pip install \
    numpy==1.24.4 \
    pandas==1.5.3 \
    pyarrow==11.0.0 \
    thrift==0.16.0 \
    databricks-sdk==0.9.0 \
    databricks-sql-connector==2.9.3 \
    dbt-core==1.6.0 \
    dbt-databricks==1.6.0 \
    dbt-spark==1.6.0

WORKDIR /usr/app/dbt

ENTRYPOINT ["tail", "-f", "/dev/null"]