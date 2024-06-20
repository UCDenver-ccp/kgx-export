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
FAILURE_EMAIL = os.environ.get('FAILURE_EMAIL')
START_DATE=datetime(2024, 3, 29, 0, 0)
CHUNK_SIZE = '100000'
EVIDENCE_LIMIT = '5'
STEP_SIZE = 75000


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': START_DATE,
    'schedule_interval': '0 23 * * 6',
    'email': [FAILURE_EMAIL],
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


with models.DAG(dag_id='targeted-export', default_args=default_args, catchup=True) as dag:
    filename_list = []
    export_task_list = []

    export_nodes = KubernetesPodOperator(
        task_id='targeted-nodes',
        name='nodes',
        config_file="/home/airflow/composer_kube_config",
        namespace='composer-user-workloads',
        image_pull_policy='Always',
        arguments=['-t', 'nodes', '-b', TMP_BUCKET],
        env_vars={
            'MYSQL_DATABASE_PASSWORD': MYSQL_DATABASE_PASSWORD,
            'MYSQL_DATABASE_USER': MYSQL_DATABASE_USER,
            'MYSQL_DATABASE_INSTANCE': MYSQL_DATABASE_INSTANCE,
        },
        image='gcr.io/translator-text-workflow-dev/kgx-export:latest')
    export_edges = KubernetesPodOperator(
            task_id=f'targeted-edges',
            name=f'edge-export',
            config_file="/home/airflow/composer_kube_config",
            namespace='composer-user-workloads',
            image_pull_policy='Always',
            startup_timeout_seconds=1200,
            arguments=['-t', 'edges', '-b', TMP_BUCKET, '--chunk_size', CHUNK_SIZE, '--limit', EVIDENCE_LIMIT],
            env_vars={
                'MYSQL_DATABASE_PASSWORD': MYSQL_DATABASE_PASSWORD,
                'MYSQL_DATABASE_USER': MYSQL_DATABASE_USER,
                'MYSQL_DATABASE_INSTANCE': MYSQL_DATABASE_INSTANCE,
            },
            container_resources=k8s_models.V1ResourceRequirements(
                limits={"memory": "1G", "cpu": "1000m"},
            ),
            retries=1,
            image='gcr.io/translator-text-workflow-dev/kgx-export:latest'
        )
    generate_metadata = KubernetesPodOperator(
        task_id='targeted-metadata',
        name='targeted-metadata',
        config_file="/home/airflow/composer_kube_config",
        namespace='composer-user-workloads',
        image_pull_policy='Always',
        arguments=['-t', 'metadata', '-b', TMP_BUCKET],
        image='gcr.io/translator-text-workflow-dev/kgx-export:latest')
    generate_bte_operations = PythonOperator(
        task_id='generate_bte_operations',
        python_callable=output_operations,
        provide_context=True,
        op_kwargs={'edges_filename': '/home/airflow/gcs/data/kgx-export/edges.tsv',
                   'output_filename': '/home/airflow/gcs/data/kgx-export/operations.json'},
        dag=dag)
    compress_edge_file = BashOperator(
        task_id='targeted-compress',
        bash_command=f"cd /home/airflow/gcs/data/kgx-export/ && gzip -f edges.tsv")
    publish_files = BashOperator(
        task_id='targeted-publish',
        bash_command=f"gsutil cp gs://{TMP_BUCKET}/data/kgx-export/* gs://{UNI_BUCKET}/kgx/UniProt/")

    export_nodes >> export_edges >> generate_bte_operations >> compress_edge_file >> generate_metadata >> publish_files
