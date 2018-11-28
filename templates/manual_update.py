# -*- coding: utf-8 -*-

"""{{ name }}"""

import os.path as osp
from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.operators.ddf_plugin import ValidateDatasetDependOnGitOperator, DependencyDatasetSensor, S3UploadOperator

# steps:
# validate the dataset and done.

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': {{ datetime }},
    'retry_delay': timedelta(minutes=5),
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    'priority_weight': {{ priority }},
    'weight_rule': 'absolute',
    # 'end_date': datetime(2016, 1, 1),
    'poke_interval': 300,
    'execution_timeout': timedelta(hours=10)     # 10 hours
}

target_dataset = '{{ name }}'

# variables
datasets_dir = Variable.get('datasets_dir')
airflow_home = Variable.get('airflow_home')
s3_datasets = [x.strip() for x in Variable.get('s3_datasets').split('\n')]

logpath = osp.join(airflow_home, 'validation-log')
out_dir = osp.join(datasets_dir, target_dataset)
dag_id = target_dataset.replace('/', '_')
sub_dag_id = dag_id + '.' + 'dependency_check'

# now define the DAG
dag = DAG(dag_id, default_args=default_args,
          schedule_interval='30 0 * * *')


def get_dep_task_time(n, minutes=0):
    newdate = datetime(n.year, n.month, n.day, 0, 0)
    return newdate + timedelta(minutes=minutes)


dependency_task = DependencyDatasetSensor(task_id='update_datasets', dag=dag,
                                          external_dag_id='update_all_datasets',
                                          external_task_id='refresh_dags', pool='dependency_checking')

validate_ddf = ValidateDatasetDependOnGitOperator(task_id='validate', dag=dag,
                                                  pool='etl',
                                                  dataset=out_dir,
                                                  logpath=logpath)

# set dependencies
dependency_task >> validate_ddf


if target_dataset in s3_datasets:
    bucket = f"s3://waffle-server-dev/{target_dataset}/master-HEAD/"
    s3_upload = S3UploadOperator(dag=dag, task_id='upload_to_S3', dataset=out_dir,
                                 branch='master', bucket=bucket)

    s3_upload.set_upstream(validate_ddf)
