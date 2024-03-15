import os
import json
import gzip
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from datetime import datetime, timedelta
from airflow import models
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import (
    KubernetesPodOperator,
)
from kubernetes.client import models as k8s_models

MYSQL_DATABASE_PASSWORD=os.environ.get('MYSQL_DATABASE_PASSWORD')
MYSQL_DATABASE_USER=os.environ.get('MYSQL_DATABASE_USER')
MYSQL_DATABASE_INSTANCE=os.environ.get('MYSQL_DATABASE_INSTANCE')
PR_BUCKET = os.environ.get('PR_BUCKET')
UNI_BUCKET = os.environ.get('UNI_BUCKET')
TMP_BUCKET = os.environ.get('TMP_BUCKET')
START_DATE=datetime(2023, 8, 7, 0, 0)
CHUNK_SIZE = '1000'
EVIDENCE_LIMIT = '5'
STEP_SIZE = 75000


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': START_DATE,
    'schedule_interval': '0 23 * * *',
    'email': ['edgargaticaCU@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 0
}

def output_operations(**kwargs):
    operations_dict = {}
    with open(kwargs['edges_filename'], 'r') as infile:
        for line in infile:
            columns = line.split('\t')
            if len(columns) < 13:
                print('not enough columns')
                print(line)
                continue
            subject_namespace = columns[0].split(':')[0]
            object_namespace = columns[2].split(':')[0]
            predicate = 'no_predicate'
            if columns[1] == 'biolink:treats':
                predicate = 'treats'
            elif columns[1] == 'biolink:contributes_to':
                predicate = 'contributes_to'
            elif columns[1] == 'biolink:affects':
                if columns[3] == 'biolink:causes':
                    if columns[8] == 'activity_or_abundance' and columns[9] == 'increased':
                        predicate = 'positively_regulates'
                    elif columns[8] == 'activity_or_abundance' and columns[9] == 'decreased':
                        predicate = 'negatively_regulates'
                elif columns[3] == 'biolink:contributes_to':
                    if columns[7] == 'gain_of_function_variant_form':
                        predicate = 'gain_of_function_contributes_to'
                    elif columns[7] == 'loss_of_function_variant_form':
                        predicate = 'loss_of_function_contributes_to'
            key = subject_namespace + '_' + predicate + '_' + object_namespace
            if key in operations_dict:
                operations_dict[key] += 1
            else:
                operations_dict[key] = 1
    with open(kwargs['output_filename'], 'w') as outfile:
        x = outfile.write(json.dumps(operations_dict))

with models.DAG(dag_id='targeted-parallel', default_args=default_args, catchup=True) as dag:
    filename_list = []
    export_task_list = []
    # This creates as many pods as needed to export all assertion records in groups of STEP_SIZE, which are then run in
    # parallel as much as possible. The current infrastructure seems to support a maximum of 15 simultaneous workflows,
    # but I have had disconnects when something runs "too long". The task finishes, but is reported as failure to
    # Airflow so nothing downstream runs. So it's better to have 30 shorter tasks that effectively run in two waves
    # rather than 15 longer tasks that run all at once.
    # TODO: the upper limit on the range needs to be the total number of assertion records
    for i in range(0, 2400000, STEP_SIZE):
        filename_list.append(f'edges_{i}_{i + STEP_SIZE}.tsv')
        export_task_list.append(KubernetesPodOperator(
            task_id=f'targeted-edges-{i}',
            name=f'parallel-{i}',
            config_file="/home/airflow/composer_kube_config",
            namespace='composer-user-workloads',
            image_pull_policy='Always',
            startup_timeout_seconds=1200,
            arguments=['-t', 'edges', '-uni', TMP_BUCKET,
                       '--chunk_size', CHUNK_SIZE, '--limit', EVIDENCE_LIMIT,
                       '--assertion_offset', f'{i}', '--assertion_limit', f'{STEP_SIZE}'],
            env_vars={
                'MYSQL_DATABASE_PASSWORD': MYSQL_DATABASE_PASSWORD,
                'MYSQL_DATABASE_USER': MYSQL_DATABASE_USER,
                'MYSQL_DATABASE_INSTANCE': MYSQL_DATABASE_INSTANCE,
            },
            container_resources=k8s_models.V1ResourceRequirements(
                limits={"memory": "1G", "cpu": "1000m"},
            ),
            retries=1,
            image='gcr.io/translator-text-workflow-dev/kgx-export-parallel:latest'
        ))
    export_nodes = KubernetesPodOperator(
        task_id='targeted-nodes',
        name='nodes',
        config_file="/home/airflow/composer_kube_config",
        namespace='composer-user-workloads',
        image_pull_policy='Always',
        arguments=['-t', 'nodes', '-uni', TMP_BUCKET],
        env_vars={
            'MYSQL_DATABASE_PASSWORD': MYSQL_DATABASE_PASSWORD,
            'MYSQL_DATABASE_USER': MYSQL_DATABASE_USER,
            'MYSQL_DATABASE_INSTANCE': MYSQL_DATABASE_INSTANCE,
        },
        image='gcr.io/translator-text-workflow-dev/kgx-export-parallel:latest')
    generate_metadata = KubernetesPodOperator(
        task_id='targeted-metadata',
        name='targeted-metadata',
        config_file="/home/airflow/composer_kube_config",
        namespace='composer-user-workloads',
        image_pull_policy='Always',
        arguments=['-t', 'metadata', '-uni', TMP_BUCKET],
        image='gcr.io/translator-text-workflow-dev/kgx-export-parallel:latest')
    generate_bte_operations = PythonOperator(
        task_id='generate_bte_operations',
        python_callable=output_operations,
        provide_context=True,
        op_kwargs={'edges_filename': '/home/airflow/gcs/data/kgx-export/edges.tsv',
                   'output_filename': '/home/airflow/gcs/data/kgx-export/operations.json'},
        dag=dag)
    combine_files = BashOperator(
        task_id='targeted-compose',
        bash_command=f"cd /home/airflow/gcs/data/kgx-export/ && cat {' '.join(filename_list)} > edges.tsv")
    compress_edge_file = BashOperator(
        task_id='targeted-compress',
        bash_command=f"cd /home/airflow/gcs/data/kgx-export/ && gzip -f edges.tsv")
    cleanup_files = BashOperator(
        task_id='targeted-cleanup',
        bash_command=f"cd /home/airflow/gcs/data/kgx-export/ && rm {' '.join(filename_list)}")
    publish_files = BashOperator(
        task_id='targeted-publish',
        bash_command=f"gsutil cp gs://{TMP_BUCKET}/data/kgx-export/* gs://{UNI_BUCKET}/kgx/UniProt/")

    export_nodes >> export_task_list >> combine_files >> generate_bte_operations >> compress_edge_file >> cleanup_files >> generate_metadata >> publish_files
