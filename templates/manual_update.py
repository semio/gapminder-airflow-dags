# -*- coding: utf-8 -*-

"""{{ name }}"""

import os.path as osp
from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.operators import ValidateDatasetOperator
from airflow.operators.sensors import ExternalTaskSensor

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
    # 'end_date': datetime(2016, 1, 1),
}

target_dataset = 'open-numbers/{{ name }}'

datasets_dir = Variable.get('datasets_dir')

out_dir = osp.join(datasets_dir, target_dataset)
dag_id = target_dataset.replace('/', '_')
sub_dag_id = dag_id + '.' + 'dependency_check'

# now define the DAG
dag = DAG(dag_id, default_args=default_args,
          schedule_interval='10 0 * * *')


dependency_task = ExternalTaskSensor(task_id='update_datasets', dag=dag,
                                     external_dag_id='update_all_datasets',
                                     external_task_id='update_all_dataset')

validate_ddf = ValidateDatasetOperator(task_id='validate', dag=dag,
                                       pool='etl',
                                       dataset=out_dir)

# set dependencies
dependency_task >> validate_ddf
