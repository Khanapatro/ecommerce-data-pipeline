from datetime import timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago

# =====================================================
# DEFAULT ARGS
# =====================================================
default_args = {
    'owner': 'ecommerce-pipeline',
    'depends_on_past': False,
    #You can add your email
    'email': ['khanapatro84@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

# =====================================================
# DAG DEFINITION
# =====================================================
with DAG(
    dag_id='ecommerce_pipeline',
    default_args=default_args,
    description='E-Commerce Sales Data Pipeline: Kafka → ADLS → Databricks → dbt',
    schedule_interval='0 6 * * *',
    start_date=days_ago(1),
    catchup=False,
    tags=['ecommerce', 'databricks', 'dbt'],
) as dag:

    # =====================================================
    # TASK 1 — BRONZE LAYER
    # =====================================================
    bronze_ingest = BashOperator(
        task_id='bronze_ingest',
        retries=3,
        retry_delay=timedelta(minutes=5),
        bash_command="""
        curl -X POST \
        https://adb-7405615288290811.11.azuredatabricks.net/api/2.1/jobs/runs/submit \
        -H "Authorization: Bearer {{ var.value.DATABRICKS_TOKEN }}" \
        -H "Content-Type: application/json" \
        -d '{
            "run_name": "bronze_ingest_{{ ds }}",
            "existing_cluster_id": "{{ var.value.DATABRICKS_CLUSTER_ID }}",
            "notebook_task": {
                "notebook_path": "/Users/khanapatro88@outlook.com/.bundle/Projects/dev/files/E-COMMERECEPIPELINE/databricks/bronze/01_ingest_raw",
                "base_parameters": {
                    "execution_date": "{{ ds }}"
                }
            }
        }'
        """,
    )

    # =====================================================
    # TASK 2 — SILVER LAYER (CLEAN ORDERS)
    # =====================================================
    silver_clean_orders = BashOperator(
        task_id='silver_clean_orders',
        retries=2,
        retry_delay=timedelta(minutes=5),
        bash_command="""
        curl -X POST \
        https://adb-7405615288290811.11.azuredatabricks.net/api/2.1/jobs/runs/submit \
        -H "Authorization: Bearer {{ var.value.DATABRICKS_TOKEN }}" \
        -H "Content-Type: application/json" \
        -d '{
            "run_name": "silver_clean_orders_{{ ds }}",
            "existing_cluster_id": "{{ var.value.DATABRICKS_CLUSTER_ID }}",
            "notebook_task": {
                "notebook_path": "/Users/khanapatro88@outlook.com/.bundle/Projects/dev/files/E-COMMERECEPIPELINE/databricks/silver/clean_orders",
                "base_parameters": {
                    "execution_date": "{{ ds }}"
                }
            }
        }'
        """,
    )

    # =====================================================
    # TASK 3 — SILVER LAYER (CLEAN PAYMENTS)
    # =====================================================
    silver_clean_payments = BashOperator(
        task_id='silver_clean_payments',
        retries=2,
        retry_delay=timedelta(minutes=5),
        bash_command="""
        curl -X POST \
        https://adb-7405615288290811.11.azuredatabricks.net/api/2.1/jobs/runs/submit \
        -H "Authorization: Bearer {{ var.value.DATABRICKS_TOKEN }}" \
        -H "Content-Type: application/json" \
        -d '{
            "run_name": "silver_clean_payments_{{ ds }}",
            "existing_cluster_id": "{{ var.value.DATABRICKS_CLUSTER_ID }}",
            "notebook_task": {
                "notebook_path": "/Users/khanapatro88@outlook.com/.bundle/Projects/dev/files/E-COMMERECEPIPELINE/databricks/silver/clean_payments",
                "base_parameters": {
                    "execution_date": "{{ ds }}"
                }
            }
        }'
        """,
    )

    # =====================================================
    # TASK 4 — SILVER LAYER (CLEAN CUSTOMERS)
    # =====================================================
    silver_clean_customers = BashOperator(
        task_id='silver_clean_customers',
        retries=2,
        retry_delay=timedelta(minutes=5),
        bash_command="""
        curl -X POST \
        https://adb-7405615288290811.11.azuredatabricks.net/api/2.1/jobs/runs/submit \
        -H "Authorization: Bearer {{ var.value.DATABRICKS_TOKEN }}" \
        -H "Content-Type: application/json" \
        -d '{
            "run_name": "silver_clean_customers_{{ ds }}",
            "existing_cluster_id": "{{ var.value.DATABRICKS_CLUSTER_ID }}",
            "notebook_task": {
                "notebook_path": "/Users/khanapatro88@outlook.com/.bundle/Projects/dev/files/E-COMMERECEPIPELINE/databricks/silver/clean_customers",
                "base_parameters": {
                    "execution_date": "{{ ds }}"
                }
            }
        }'
        """,
    )

    # =====================================================
    # TASK 5 — SILVER LAYER (CLEAN INVENTORY)
    # =====================================================
    silver_clean_inventory = BashOperator(
        task_id='silver_clean_inventory',
        retries=2,
        retry_delay=timedelta(minutes=5),
        bash_command="""
        curl -X POST \
        https://adb-7405615288290811.11.azuredatabricks.net/api/2.1/jobs/runs/submit \
        -H "Authorization: Bearer {{ var.value.DATABRICKS_TOKEN }}" \
        -H "Content-Type: application/json" \
        -d '{
            "run_name": "silver_clean_inventory_{{ ds }}",
            "existing_cluster_id": "{{ var.value.DATABRICKS_CLUSTER_ID }}",
            "notebook_task": {
                "notebook_path": "/Users/khanapatro88@outlook.com/.bundle/Projects/dev/files/E-COMMERECEPIPELINE/databricks/silver/clean_inventory",
                "base_parameters": {
                    "execution_date": "{{ ds }}"
                }
            }
        }'
        """,
    )

    # =====================================================
    # TASK 6 — GOLD LAYER (DAILY REVENUE)
    # =====================================================
    gold_daily_revenue = BashOperator(
        task_id='gold_daily_revenue',
        retries=2,
        retry_delay=timedelta(minutes=5),
        bash_command="""
        curl -X POST \
        https://adb-7405615288290811.11.azuredatabricks.net/api/2.1/jobs/runs/submit \
        -H "Authorization: Bearer {{ var.value.DATABRICKS_TOKEN }}" \
        -H "Content-Type: application/json" \
        -d '{
            "run_name": "gold_daily_revenue_{{ ds }}",
            "existing_cluster_id": "{{ var.value.DATABRICKS_CLUSTER_ID }}",
            "notebook_task": {
                "notebook_path": "/Users/khanapatro88@outlook.com/.bundle/Projects/dev/files/E-COMMERECEPIPELINE/databricks/gold/daily_revenue",
                "base_parameters": {
                    "execution_date": "{{ ds }}"
                }
            }
        }'
        """,
    )

    # =====================================================
    # TASK 7 — GOLD LAYER (TOP PRODUCTS)
    # =====================================================
    gold_top_products = BashOperator(
        task_id='gold_top_products',
        retries=2,
        retry_delay=timedelta(minutes=5),
        bash_command="""
        curl -X POST \
        https://adb-7405615288290811.11.azuredatabricks.net/api/2.1/jobs/runs/submit \
        -H "Authorization: Bearer {{ var.value.DATABRICKS_TOKEN }}" \
        -H "Content-Type: application/json" \
        -d '{
            "run_name": "gold_top_products_{{ ds }}",
            "existing_cluster_id": "{{ var.value.DATABRICKS_CLUSTER_ID }}",
            "notebook_task": {
                "notebook_path": "/Users/khanapatro88@outlook.com/.bundle/Projects/dev/files/E-COMMERECEPIPELINE/databricks/gold/top_products",
                "base_parameters": {
                    "execution_date": "{{ ds }}"
                }
            }
        }'
        """,
    )

    # =====================================================
    # TASK 8 — GOLD LAYER (CUSTOMER LTV)
    # =====================================================
    gold_customer_ltv = BashOperator(
        task_id='gold_customer_ltv',
        retries=2,
        retry_delay=timedelta(minutes=5),
        bash_command="""
        curl -X POST \
        https://adb-7405615288290811.11.azuredatabricks.net/api/2.1/jobs/runs/submit \
        -H "Authorization: Bearer {{ var.value.DATABRICKS_TOKEN }}" \
        -H "Content-Type: application/json" \
        -d '{
            "run_name": "gold_customer_ltv_{{ ds }}",
            "existing_cluster_id": "{{ var.value.DATABRICKS_CLUSTER_ID }}",
            "notebook_task": {
                "notebook_path": "/Users/khanapatro88@outlook.com/.bundle/Projects/dev/files/E-COMMERECEPIPELINE/databricks/gold/customer_ltv",
                "base_parameters": {
                    "execution_date": "{{ ds }}"
                }
            }
        }'
        """,
    )

    # =====================================================
    # TASK 9 — GOLD LAYER (PAYMENT SUMMARY)
    # =====================================================
    gold_payment_summary = BashOperator(
        task_id='gold_payment_summary',
        retries=2,
        retry_delay=timedelta(minutes=5),
        bash_command="""
        curl -X POST \
        https://adb-7405615288290811.11.azuredatabricks.net/api/2.1/jobs/runs/submit \
        -H "Authorization: Bearer {{ var.value.DATABRICKS_TOKEN }}" \
        -H "Content-Type: application/json" \
        -d '{
            "run_name": "gold_payment_summary_{{ ds }}",
            "existing_cluster_id": "{{ var.value.DATABRICKS_CLUSTER_ID }}",
            "notebook_task": {
                "notebook_path": "/Users/khanapatro88@outlook.com/.bundle/Projects/dev/files/E-COMMERECEPIPELINE/databricks/gold/payment_summary",
                "base_parameters": {
                    "execution_date": "{{ ds }}"
                }
            }
        }'
        """,
    )

    # =====================================================
    # TASK 10 — DBT RUN
    # =====================================================
    dbt_run = BashOperator(
        task_id='dbt_run',
        retries=2,
        retry_delay=timedelta(minutes=5),
        bash_command='docker exec dbt dbt run --project-dir /usr/app/dbt',
    )

    # =====================================================
    # TASK 11 — DBT TEST
    # =====================================================
    dbt_test = BashOperator(
        task_id='dbt_test',
        retries=1,
        retry_delay=timedelta(minutes=5),
        bash_command='docker exec dbt dbt test --project-dir /usr/app/dbt',
    )

    # =====================================================
    # TASK 12 — PIPELINE SUCCESS NOTIFICATION
    # =====================================================
    notify_success = BashOperator(
        task_id='notify_success',
        bash_command='echo "Pipeline completed successfully for {{ ds }}"',
        trigger_rule='all_success',
    )

    # =====================================================
    # TASK 13 — PIPELINE FAILURE NOTIFICATION
    # =====================================================
    notify_failure = BashOperator(
        task_id='notify_failure',
        bash_command='echo "Pipeline FAILED for {{ ds }} — check Airflow logs"',
        trigger_rule='one_failed',
    )

    # =====================================================
    # TASK DEPENDENCIES
    # =====================================================

    # bronze → silver (parallel)
    bronze_ingest >> [
        silver_clean_orders,
        silver_clean_payments,
        silver_clean_customers,
        silver_clean_inventory
    ]

    # silver → gold (each silver connects to each gold)
    for silver_task in [
        silver_clean_orders,
        silver_clean_payments,
        silver_clean_customers,
        silver_clean_inventory
    ]:
        for gold_task in [
            gold_daily_revenue,
            gold_top_products,
            gold_customer_ltv,
            gold_payment_summary
        ]:
            silver_task >> gold_task

    # gold → dbt_run
    [
        gold_daily_revenue,
        gold_top_products,
        gold_customer_ltv,
        gold_payment_summary
    ] >> dbt_run

    # dbt_run → dbt_test
    dbt_run >> dbt_test

    # notifications
    dbt_test >> notify_success
    dbt_test >> notify_failure
