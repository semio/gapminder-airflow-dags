# -*- coding: utf-8 -*-

"""{{ name }}"""

import os.path as osp
from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.operators import (GenerateDatapackageOperator,
                               GitCheckoutOperator, GitPushOperator,
                               RunETLOperator, ValidateDatasetOperator)
from airflow.operators.sensors import ExternalTaskSensor
from airflow.operators.subdag_operator import SubDagOperator

from functools import partial

# steps:
# 1. checkout the airflow branch
# 2. run etl.py
# 3. generate datapackage
# 4. validate-ddf
# 4. if there are updates, push

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': {{ datetime }},
    'retry_delay': timedelta(minutes=5),
    # 'queue': 'bash_queue',
    # 'pool': 'etl',
    'priority_weight': {{ priority }},
    # 'end_date': datetime(2016, 1, 1),
    'poke_interval': 300
}

target_dataset = 'open-numbers/{{ name }}'
depends_on = {{ dependencies }}

datasets_dir = Variable.get('datasets_dir')

out_dir = osp.join(datasets_dir, target_dataset)
dag_id = target_dataset.replace('/', '_')
sub_dag_id = dag_id + '.' + 'dependency_check'

# now define the DAG
dag = DAG(dag_id, default_args=default_args,
          schedule_interval='10 0 * * *')


def sub_dag():
    args = {
        'owner': 'airflow',
        'depends_on_past': False,
        'start_date': {{ datetime }},
        'retry_delay': timedelta(minutes=5),
        'poke_interval': 300
    }
    subdag = DAG(sub_dag_id, default_args=args, schedule_interval='@once')

    dep_tasks = []

    def get_dep_task_time(n, minutes=0):
        if minutes !=0:
            return n.date() + timedelta(minutes=minutes)
        return n.date()

    update_datasets = ExternalTaskSensor(task_id='update_datasets', dag=subdag,
                                         external_dag_id='update_all_datasets',
                                         external_task_id='update_all_dataset',
                                         execution_date_fn=get_dep_task_time)

    for dep in depends_on:
        t = ExternalTaskSensor(task_id='wait_for_{}'.format(dep).replace('/', '_'),
                               dag=subdag,
                               allowed_states=['success'],
                               external_dag_id=dep.replace('/', '_'),
                               external_task_id='validate',
                               execution_date_fn=partial(get_dep_task_time, minutes=10))
        dep_tasks.append(t)

    return subdag


dependency_task = SubDagOperator(subdag=sub_dag(), task_id='dependency_check', dag=dag)

checkout_task = GitCheckoutOperator(task_id='checkout_airflow_branch', dag=dag,
                                    dataset=out_dir, version='autogenerated')
recipe_task = RunETLOperator(task_id='run_etl', dag=dag,
                             pool='etl',
                             dataset=out_dir)
datapackage_task = GenerateDatapackageOperator(task_id='generate_datapackage', dag=dag,
                                               pool='etl',
                                               dataset=out_dir)
validate_ddf = ValidateDatasetOperator(task_id='validate', dag=dag,
                                       pool='etl',
                                       dataset=out_dir)
git_push_task = GitPushOperator(task_id='git_push', dag=dag,
                                dataset=out_dir)

# set dependencies
(dependency_task >>
 checkout_task >>
 recipe_task >>
 datapackage_task >>
 validate_ddf >>
 git_push_task)
