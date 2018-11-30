# -*- coding: utf-8 -*-

"""{{ name }}"""

import os.path as osp
from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.operators.ddf_plugin import (GenerateDatapackageOperator,
                                          DependencyDatasetSensor,
                                          UpdateSourceOperator,
                                          GitCheckoutOperator, GitPushOperator,
                                          GitMergeOperator, RunETLOperator,
                                          S3UploadOperator, ValidateDatasetOperator)
from airflow.operators.subdag_operator import SubDagOperator
from airflow.executors.local_executor import LocalExecutor

from functools import partial
import logging

# steps:
# - merge autogenerated branch to master
# - git push
# - upload to S3

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
    'execution_timeout': timedelta(hours=10),     # 10 hours
    'weight_rule': 'absolute'
}

target_dataset = '{{ name }}'

# variables
datasets_dir = Variable.get('datasets_dir')
airflow_home = Variable.get('airflow_home')

dag_id = target_dataset.replace('/', '_') + "_production"
sub_dag_id = dag_id + '.' + 'dependency_check'
out_dir = osp.join(datasets_dir, target_dataset)

# now define the DAG
schedule = '@once'

dag = DAG(dag_id, default_args=default_args,
          schedule_interval=schedule)


def sub_dag():
    args = {
        'owner': 'airflow',
        'depends_on_past': False,
        'start_date': {{ datetime }},
        'retry_delay': timedelta(minutes=5),
        'poke_interval': 60 * 10,
        'timeout': 60 * 60 * 8,
        'priority_weight': {{ priority }},
        'weight_rule': 'absolute',
        'pool': 'dependency_checking'
    }
    subdag = DAG(sub_dag_id, default_args=args, schedule_interval='@once')

    # prevent running when the other DAG for autogenerated branch is running
    check_other_dag = DependencyDatasetSensor(task_id="check_other_dag", dag=subdag,
                                              external_dag_id=target_dataset.replace('/', '_'),
                                              external_task_id='validate')

    return subdag


dependency_task = SubDagOperator(subdag=sub_dag(), task_id='dependency_check', dag=dag, executor=LocalExecutor(parallelism=2))

# checkout_task = GitMergeOperator(task_id='checkout_update_airflow_branch', dag=dag,
#                                     dataset=out_dir, head='develop', base='autogenerated')
git_merge_task = GitMergeOperator(task_id='merge_into_master', dag=dag,
                                  dataset=out_dir, base='master', head='autogenerated')
git_push_task = GitPushOperator(task_id='git_push', dag=dag,
                                pool='etl',
                                dataset=out_dir)

bucket = f"s3://waffle-server/{target_dataset}/master/head/"
s3_upload = S3UploadOperator(dag=dag, task_id='upload_to_S3', dataset=out_dir,
                             branch='master', bucket=bucket)

# set dependencies
(dependency_task >>
 # checkout_task >>
 git_merge_task >>
 git_push_task >>
 s3_upload)
