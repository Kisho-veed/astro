from airflow.decorators import dag
from airflow.models.baseoperator import chain
from airflow.providers.google.cloud.sensors.bigquery import BigQueryTableExistenceSensor
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryCreateEmptyDatasetOperator,
    BigQueryCreateEmptyTableOperator,
    BigQueryInsertJobOperator,
    BigQueryValueCheckOperator,
)
from airflow.utils.dates import datetime

# google_cloud_default
# /usr/local/airflow/include/gcp/service_account.json

DATASET = "testing_dataset"
TABLE = "forestfires"
PROJECT_ID = "testingairflow123"


@dag(
    "simple_bigquery_run",
    start_date=datetime(2021, 1, 1),
    description="Example DAG showcasing loading and data quality checking with BigQuery.",
    doc_md=__doc__,
    schedule_interval=None,
    catchup=False,
)
def big_query_data_run():
    create_dataset = BigQueryCreateEmptyDatasetOperator(
        task_id="create_dataset",
        dataset_id=DATASET,
        project_id=PROJECT_ID,
    )

    create_table = BigQueryCreateEmptyTableOperator(
        task_id="create_table",
        dataset_id=DATASET,
        table_id=TABLE,
        project_id=PROJECT_ID,
        schema_fields=[
            {"name": "id", "type": "INTEGER", "mode": "REQUIRED"},
            {"name": "y", "type": "INTEGER", "mode": "NULLABLE"},
            {"name": "month", "type": "STRING", "mode": "NULLABLE"},
            {"name": "day", "type": "STRING", "mode": "NULLABLE"},
            {"name": "ffmc", "type": "FLOAT", "mode": "NULLABLE"},
            {"name": "dmc", "type": "FLOAT", "mode": "NULLABLE"},
            {"name": "dc", "type": "FLOAT", "mode": "NULLABLE"},
            {"name": "isi", "type": "FLOAT", "mode": "NULLABLE"},
            {"name": "temp", "type": "FLOAT", "mode": "NULLABLE"},
            {"name": "rh", "type": "FLOAT", "mode": "NULLABLE"},
            {"name": "wind", "type": "FLOAT", "mode": "NULLABLE"},
            {"name": "rain", "type": "FLOAT", "mode": "NULLABLE"},
            {"name": "area", "type": "FLOAT", "mode": "NULLABLE"},
        ],
    )

    check_table_exists = BigQueryTableExistenceSensor(
        task_id="check_for_table",
        project_id=PROJECT_ID,
        dataset_id=DATASET,
        table_id=TABLE,
    )

    load_data = BigQueryInsertJobOperator(
        task_id="insert_query",
        configuration={
            "query": {
                "query": f"""
                    INSERT INTO `{PROJECT_ID}.{DATASET}.{TABLE}`
                    VALUES
                    (1,2,'aug','fri',91.0,166.9,752.6,7.1,25.9,41.0,3.6,0.0,0.0),
                    (2,2,'feb','mon',84.0,9.3,34.0,2.1,13.9,40.0,5.4,0.0,0.0),
                    (3,4,'mar','sat',69.0,2.4,15.5,0.7,17.4,24.0,5.4,0.0,0.0),
                    (4,3,'oct','tue',87.0,66.5,278.6,5.8,22.2,34.0,3.6,0.0,0.0)
                """,
                "useLegacySql": False,
            }
        },
    )

    check_bq_row_count = BigQueryValueCheckOperator(
        task_id="check_row_count",
        sql=f"SELECT COUNT(*) FROM `{PROJECT_ID}.{DATASET}.{TABLE}`",
        pass_value=4,
        use_legacy_sql=False,
    )

    (
        create_dataset
        >> create_table
        >> check_table_exists
        >> load_data
        >> check_bq_row_count
    )


big_query_data_run()
