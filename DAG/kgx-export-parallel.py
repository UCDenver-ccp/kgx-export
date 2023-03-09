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


with models.DAG(dag_id='targeted-parallel', default_args=default_args,
                schedule_interval=timedelta(days=1), start_date=START_DATE, catchup=False) as dag:
    filename_list = []
    export_task_list = []
    # This creates as many pods as needed to export all assertion records in groups of STEP_SIZE, which are then run in
    # parallel as much as possible. The current infrastructure seems to support a maximum of 15 simultaneous workflows,
    # but I have had disconnects when something runs "too long". The task finishes, but is reported as failure to
    # Airflow so nothing downstream runs. So it's better to have 30 shorter tasks that effectively run in two waves
    # rather than 15 longer tasks that run all at once.
    # TODO: the upper limit on the range needs to be the total number of assertion records
    for i in range(0, 2400000, STEP_SIZE):
        filename_list.append(f'gs://{UNI_BUCKET}/kgx/UniProt/edges_{i}_{i + STEP_SIZE}.tsv')
        export_task_list.append(kubernetes_pod_operator.KubernetesPodOperator(
            task_id=f'targeted-edges-{i}',
            name=f'parallel-{i}',
            namespace='default',
            image_pull_policy='Always',
            arguments=['-t', 'edges', '-uni', UNI_BUCKET,
                       '--chunk_size', CHUNK_SIZE, '--limit', EVIDENCE_LIMIT,
                       '--assertion_offset', f'{i}', '--assertion_limit', f'{STEP_SIZE}'],
            env_vars={
                'MYSQL_DATABASE_PASSWORD': MYSQL_DATABASE_PASSWORD,
                'MYSQL_DATABASE_USER': MYSQL_DATABASE_USER,
                'MYSQL_DATABASE_INSTANCE': MYSQL_DATABASE_INSTANCE,
            },
            image='gcr.io/translator-text-workflow-dev/kgx-export-parallel:latest'
        ))
    export_nodes = kubernetes_pod_operator.KubernetesPodOperator(
        task_id='targeted-nodes',
        name='nodes',
        namespace='default',
        image_pull_policy='Always',
        arguments=['-t', 'nodes', '-uni', UNI_BUCKET],
        env_vars={
            'MYSQL_DATABASE_PASSWORD': MYSQL_DATABASE_PASSWORD,
            'MYSQL_DATABASE_USER': MYSQL_DATABASE_USER,
            'MYSQL_DATABASE_INSTANCE': MYSQL_DATABASE_INSTANCE,
        },
        image='gcr.io/translator-text-workflow-dev/kgx-export-parallel:latest')
    generate_metadata = kubernetes_pod_operator.KubernetesPodOperator(
        task_id='targeted-metadata',
        name='targeted-metadata',
        namespace='default',
        image_pull_policy='Always',
        arguments=['-t', 'metadata', '-uni', UNI_BUCKET],
        image='gcr.io/translator-text-workflow-dev/kgx-export-parallel:latest')
    combine_files = BashOperator(
        task_id='targeted-compose',
        bash_command=f"gsutil compose {' '.join(filename_list)} gs://{UNI_BUCKET}/kgx/UniProt/edges.tsv")
    cleanup_files = BashOperator(
        task_id='targeted-cleanup',
        bash_command=f"gsutil rm {' '.join(filename_list)} gs://{UNI_BUCKET}/kgx/UniProt/edges.tsv")

    export_nodes >> export_task_list >> combine_files >> generate_metadata >> cleanup_files
