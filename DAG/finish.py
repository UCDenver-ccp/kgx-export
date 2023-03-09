import os
import gzip
from airflow.operators.bash_operator import BashOperator
from datetime import datetime, timedelta
from airflow import models
from airflow.contrib.operators import kubernetes_pod_operator

MYSQL_DATABASE_PASSWORD=os.environ.get('MYSQL_DATABASE_PASSWORD')
MYSQL_DATABASE_USER=os.environ.get('MYSQL_DATABASE_USER')
MYSQL_DATABASE_INSTANCE=os.environ.get('MYSQL_DATABASE_INSTANCE')
PR_BUCKET = os.environ.get('PR_BUCKET')
UNI_BUCKET = os.environ.get('UNI_BUCKET')
START_DATE=datetime(2023, 2, 23, 0, 0)
ONTOLOGY = 'uniprot'
CHUNK_SIZE = '1000'
EVIDENCE_LIMIT = '5'
STEP_SIZE = 80000


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': START_DATE,
    'email': ['edgargaticaCU@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0
}

# Sometimes the export DAG has failed to complete all steps because of disconnects between the Airflow manager and the
# workflows themselves. Usually this results in all the exports working correctly but reporting failure, which causes
# the downstream tasks not to run at all. This DAG is just those post-export steps: combine the partial edge files,
# generate the metadata and KGE files (and compress the edges file), and remove the partial edge files.
with models.DAG(dag_id='targeted-finish', default_args=default_args,
                schedule_interval=timedelta(days=1), start_date=START_DATE, catchup=False) as dag:
    filename_list = []
    export_task_list = []
    for i in range(0, 2400000, STEP_SIZE):
        filename_list.append(f'gs://{UNI_BUCKET}/kgx/UniProt/edges_{i}_{i + STEP_SIZE}.tsv')
    generate_metadata = kubernetes_pod_operator.KubernetesPodOperator(
        task_id='finish-metadata',
        name='finish-metadata',
        namespace='default',
        image_pull_policy='Always',
        arguments=['-t', 'metadata', '-uni', UNI_BUCKET],
        image='gcr.io/translator-text-workflow-dev/kgx-export-parallel:latest')
    combine_files = BashOperator(
        task_id='finish-compose',
        bash_command=f"gsutil compose {' '.join(filename_list)} gs://{UNI_BUCKET}/kgx/UniProt/edges.tsv")
    cleanup_files = BashOperator(
        task_id='finish-cleanup',
        bash_command=f"gsutil rm {' '.join(filename_list)} gs://{UNI_BUCKET}/kgx/UniProt/edges.tsv")


    combine_files >> generate_metadata >> cleanup_files