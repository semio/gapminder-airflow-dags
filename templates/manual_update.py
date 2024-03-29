# -*- coding: utf-8 -*-

"""{{ name }}"""

import os.path as osp
from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.hooks.base_hook import BaseHook
from ddf_operators import (ValidateDatasetOperator,
                           ValidateDatasetDependOnGitOperator,
                           DependencyDatasetSensor,
                           GCSUploadOperator, SlackReportOperator,
                           GitPullOperator)

# steps:
# validate the dataset and done.

# variables
target_dataset = '{{ name }}'

datasets_dir = Variable.get('datasets_dir')
airflow_home = Variable.get('airflow_home')
# gcs_datasets = [x.strip() for x in Variable.get('gcs_datasets').split('\n')]
endpoint = BaseHook.get_connection('slack_connection').password
airflow_baseurl = BaseHook.get_connection('airflow_web').host

logpath = osp.join(airflow_home, 'validation-log')
out_dir = osp.join(datasets_dir, target_dataset)
dag_id = target_dataset.replace('/', '_')
sub_dag_id = dag_id + '.' + 'dependency_check'


def slack_report(context):
    task = SlackReportOperator(task_id='slack_report', http_conn_id='slack_connection',
                               endpoint=endpoint, status='failed', airflow_baseurl=airflow_baseurl)
    context['target_dataset'] = '{{ name }}'
    task.execute(context)


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
    'execution_timeout': timedelta(hours=10),     # 10 hours
    'on_failure_callback': slack_report
}

# now define the DAG
schedule = "{{ schedule }}"

dag = DAG(dag_id, default_args=default_args,
          schedule_interval=schedule)


def get_dep_task_time(n, minutes=0):
    newdate = datetime(n.year, n.month, n.day, 0, 0)
    return newdate + timedelta(minutes=minutes)


# dependency_task = DependencyDatasetSensor(task_id='update_datasets', dag=dag,
#                                           external_dag_id='update_all_datasets',
#                                           external_task_id='refresh_dags', pool='dependency_checking')

git_pull = GitPullOperator(task_id='git_pull', dag=dag, dataset=out_dir)

validate_ddf = ValidateDatasetOperator(task_id='validate', dag=dag,
                                       pool='etl',
                                       dataset=out_dir,
                                       logpath=logpath)

git_pull >> validate_ddf
