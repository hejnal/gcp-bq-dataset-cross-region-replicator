
from datetime import datetime
from airflow import DAG
from airflow.decorators import task

from airflow.operators.empty import EmptyOperator
from airflow.providers.google.cloud.transfers.bigquery_to_gcs import BigQueryToGCSOperator
from airflow.providers.google.cloud.operators.cloud_storage_transfer_service import (
    CloudDataTransferServiceCreateJobOperator,
    CloudDataTransferServiceListOperationsOperator
)
from plugins.sensors.CloudDataTransferServiceFinalisedJobStatusSensor import CloudDataTransferServiceFinalisedJobStatusSensor
from airflow.operators.python import get_current_context

from airflow.providers.google.cloud.hooks.cloud_storage_transfer_service import (
    GcpTransferOperationStatus,
    GcpTransferJobsStatus,
    TRANSFER_OPTIONS,
    PROJECT_ID,
    BUCKET_NAME,
    GCS_DATA_SINK,
    STATUS,
    DESCRIPTION,
    GCS_DATA_SOURCE,
    SCHEDULE_END_DATE,
    SCHEDULE_START_DATE,
    SCHEDULE,
    TRANSFER_SPEC,
    ALREADY_EXISTING_IN_SINK,
    FILTER_PROJECT_ID,
    FILTER_JOB_NAMES,
    JOB_NAME,
    STATUS,
)

from plugins.operators.CloudDataTransferServiceRunJobOperator import CloudDataTransferServiceRunJobOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator

DAG_ID = "bq_dataset_replicator"

# TODO set the project specific parameters
GCP_PROJECT_ID = "<gcp_project_id>"
GCP_DESCRIPTION="Automated transfer created from Composer"
WAIT_FOR_OPERATION_POKE_INTERVAL=30
GCP_TRANSFER_FIRST_TARGET_BUCKET="<gcp_transfer_first_target_bucket>"
GCP_TRANSFER_SECOND_TARGET_BUCKET="<gcp_transfer_second_target_bucket>"
TRANSFER_NAME="<transfer_name>"

SOURCE_PROJECT_DATASET_TABLE="<source_project_dataset_table>"
SOURCE_PROJECT_DATASET_TABLE_PARTITION="<source_project_dataset_table_partition>"
DESTINATION_CLOUD_STORAGE_URIS="<destination_cloud_storage_uris>"   # example: gs://<bucket_id>/export_from_cli_with_decorators/export*.avro

GCS_SOURCE_OBJECTS="<gcs_source_objects>"   # example: "export_from_cli_with_decorators/export*.avro"
DESTINATION_PROJECT_DATASET_TABLE="<destination_project_dataset_table>" # example target_data_with_transfer_service_netherlands.source_table_with_PII_1tb_sample_partitioned$20200101

with DAG(
    DAG_ID,
    schedule_interval='@once',  # Override to match your needs
    start_date=datetime(2022, 1, 1),
    catchup=False,  # Override to match your needs
    tags=['replicator'],
) as dag:

    start = EmptyOperator(
        task_id='start',
        dag=dag,
    )
    
    export_bq_to_gcs_task = BigQueryToGCSOperator(
        task_id="export_bq_to_gcs_task",
        source_project_dataset_table = f"{SOURCE_PROJECT_DATASET_TABLE}${SOURCE_PROJECT_DATASET_TABLE_PARTITION}",
        destination_cloud_storage_uris=DESTINATION_CLOUD_STORAGE_URIS,
        export_format="AVRO",
        compression="SNAPPY",
        gcp_conn_id="google_cloud_default",
    )
  
    get_gcs_transfer_task = CloudDataTransferServiceListOperationsOperator(
        task_id="get_gcs_transfer_task", 
        gcp_conn_id="google_cloud_default",
        request_filter={
            FILTER_PROJECT_ID: GCP_PROJECT_ID,
            FILTER_JOB_NAMES: [f"transferJobs/{TRANSFER_NAME}"],
      },
    )
    
    @task.branch(task_id="branch_based_on_the_transfer_existance_condition")
    def branch_based_on_the_transfer_existance_condition():
        context = get_current_context()
        ti = context["ti"]
        xcom_value = ti.xcom_pull(task_ids="get_gcs_transfer_task")
        if len(xcom_value) == 0:
            return "create_gcs_cross_region_transfer_job"
        else:
            return "run_gcs_cross_region_transfer_job"
        
    branch_based_on_the_transfer_existance_condition_op = branch_based_on_the_transfer_existance_condition()
    
    gcs_to_gcs_transfer_body = {
        JOB_NAME: f"transferJobs/{TRANSFER_NAME}",
        DESCRIPTION: GCP_DESCRIPTION,
        STATUS: GcpTransferJobsStatus.ENABLED,
        PROJECT_ID: GCP_PROJECT_ID,
        SCHEDULE: {
            SCHEDULE_START_DATE: datetime.now().date(),
            SCHEDULE_END_DATE: datetime(2030, 1, 1).date(),
        },
        TRANSFER_SPEC: {
            GCS_DATA_SOURCE: {BUCKET_NAME: GCP_TRANSFER_SECOND_TARGET_BUCKET},
            GCS_DATA_SINK: {BUCKET_NAME: GCP_TRANSFER_SECOND_TARGET_BUCKET},
            TRANSFER_OPTIONS: {ALREADY_EXISTING_IN_SINK: True},
        },
    }
  
    create_gcs_cross_region_transfer_job = CloudDataTransferServiceCreateJobOperator(
        task_id="create_gcs_cross_region_transfer_job", 
        body=gcs_to_gcs_transfer_body,
        gcp_conn_id="google_cloud_default",
    )
    
    run_gcs_cross_region_transfer_job = CloudDataTransferServiceRunJobOperator(
        task_id="run_gcs_cross_region_transfer_job",
        job_name=f"transferJobs/{TRANSFER_NAME}",
        body={"project_id": GCP_PROJECT_ID},
        gcp_conn_id="google_cloud_default",
    )
    
    wait_for_operation_to_end = CloudDataTransferServiceFinalisedJobStatusSensor(
        task_id="wait_for_operation_to_end",
        job_name=f"transferJobs/{TRANSFER_NAME}",
        expected_statuses={GcpTransferOperationStatus.SUCCESS, GcpTransferOperationStatus.ABORTED, GcpTransferOperationStatus.FAILED },
        mode="reschedule",
        poke_interval=WAIT_FOR_OPERATION_POKE_INTERVAL,
        gcp_conn_id="google_cloud_default",
        project_id=GCP_PROJECT_ID,
        trigger_rule="one_success",
    )
    
    load_data_to_bigquery_partition = GCSToBigQueryOperator(
        task_id="load_data_to_bigquery",
        bucket=GCP_TRANSFER_SECOND_TARGET_BUCKET,
        gcp_conn_id="google_cloud_default",
        source_objects=GCS_SOURCE_OBJECTS,
        destination_project_dataset_table=DESTINATION_PROJECT_DATASET_TABLE,
        source_format="AVRO",
        create_disposition="CREATE_IF_NEEDED",
        write_disposition="WRITE_TRUNCATE",
        autodetect=True
    )
    
    end = EmptyOperator(
        task_id='end',
        dag=dag,
    )

start >> export_bq_to_gcs_task >> get_gcs_transfer_task >> branch_based_on_the_transfer_existance_condition_op
branch_based_on_the_transfer_existance_condition_op >> [ create_gcs_cross_region_transfer_job, run_gcs_cross_region_transfer_job ] >> wait_for_operation_to_end >> load_data_to_bigquery_partition >> end