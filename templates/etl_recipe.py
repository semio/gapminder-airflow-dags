# -*- coding: utf-8 -*-

"""{{ name }}"""

import os.path as osp
from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.operators import (GenerateDatapackageOperator,
                               DependencyDatasetSensor,
                               UpdateSourceOperator,
                               GitCheckoutOperator, GitPushOperator,
                               GitMergeOperator, RunETLOperator,
                               ValidateDatasetOperator)
from airflow.operators.subdag_operator import SubDagOperator

from functools import partial
import logging

# steps:
# 1. checkout the airflow branch
# 2. merge from develop branch
# 3. run update_source.py
# 4. run etl.py
# 5. generate datapackage
# 6. validate-ddf
# 7. if there are updates, push

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': {{ datetime }},
    'retry_delay': timedelta(minutes=5),
    # 'queue': 'bash_queue',
    # 'pool': 'etl',
    'priority_weight': {{ priority }},
    # 'end_date': datetime(2016, 1, 1),
    'poke_interval': 60 * 10,  # 10 minutes
    'timeout': 60 * 60 * 8     # 8 hours
}

target_dataset = 'open-numbers/{{ name }}'
depends_on = {{ dependencies }}

# variables
datasets_dir = Variable.get('datasets_dir')
airflow_home = Variable.get('airflow_home')

logpath = osp.join(airflow_home, 'validation-log')
out_dir = osp.join(datasets_dir, target_dataset)
dag_id = target_dataset.replace('/', '_')
sub_dag_id = dag_id + '.' + 'dependency_check'

# now define the DAG
etl_type = "{{ etl_type }}"

{% if etl_type == 'recipe' %}
schedule = '10 2 * * *'
{% else %}
schedule = '10 0 * * *'
{% endif %}

dag = DAG(dag_id, default_args=default_args,
          schedule_interval=schedule)


def sub_dag():
    args = {
        'owner': 'airflow',
        'depends_on_past': False,
        'start_date': {{ datetime }},
        'retry_delay': timedelta(minutes=5),
        'poke_interval': 60 * 10,
        'timeout': 60 * 60 * 8
    }
    subdag = DAG(sub_dag_id, default_args=args, schedule_interval='@once')

    dep_tasks = []

    def get_dep_task_time(n, minutes=0, hours=0):
        newdate = datetime(n.year, n.month, n.day, 0, 0)
        if minutes !=0:
            return newdate + timedelta(minutes=minutes, hours=hours)
        return newdate

    update_datasets = DependencyDatasetSensor(task_id='update_datasets', dag=subdag,
                                              external_dag_id='update_all_datasets',
                                              external_task_id='update_all_dataset',
                                              execution_date_fn=get_dep_task_time)

    for dep, etl_type in depends_on.items():
        if etl_type == 'recipe':
            m = 10
            h = 2
        else:
            m = 10
            h = 0
        logging.info("adding the sensor task listening to {}:{}".format(h, m))
        t = DependencyDatasetSensor(task_id='wait_for_{}'.format(dep).replace('/', '_'),
                                    dag=subdag,
                                    allowed_states=['success'],
                                    external_dag_id=dep.replace('/', '_'),
                                    execution_date_fn=lambda x: get_dep_task_time(x, m, h),
                                    external_task_id='validate')
        dep_tasks.append(t)

    return subdag


dependency_task = SubDagOperator(subdag=sub_dag(), task_id='dependency_check', dag=dag)

checkout_task = GitMergeOperator(task_id='checkout_update_airflow_branch', dag=dag,
                                    dataset=out_dir, head='develop', base='autogenerated')
source_update_task = UpdateSourceOperator(task_id='run_update_source', dag=dag, dataset=out_dir)
recipe_task = RunETLOperator(task_id='run_etl', dag=dag,
                             pool='etl',
                             dataset=out_dir)
datapackage_task = GenerateDatapackageOperator(task_id='generate_datapackage', dag=dag,
                                               pool='etl',
                                               dataset=out_dir)
validate_ddf = ValidateDatasetOperator(task_id='validate', dag=dag,
                                       pool='etl',
                                       dataset=out_dir,
                                       logpath=logpath)
git_push_task = GitPushOperator(task_id='git_push', dag=dag,
                                pool='etl',
                                dataset=out_dir)

# set dependencies
(dependency_task >>
 checkout_task >>
 source_update_task >>
 recipe_task >>
 datapackage_task >>
 validate_ddf >>
 git_push_task)
