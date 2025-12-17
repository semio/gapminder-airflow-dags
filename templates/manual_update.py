# -*- coding: utf-8 -*-

"""{{ name }}"""

import os.path as osp
from datetime import timedelta

from airflow.hooks.base import BaseHook
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG, Variable

from ddf_operators import (
    GitPullOperator,
    SlackReportOperator,
    ValidateDatasetOperator,
)

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


def slack_report(context):
    task = SlackReportOperator(
        task_id='slack_report',
        http_conn_id='slack_connection',
        endpoint=endpoint,
        status='failed',
        airflow_baseurl=airflow_baseurl,
    )
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
    'execution_timeout': timedelta(hours=10),  # 10 hours
    'on_failure_callback': slack_report,
}

# now define the DAG
schedule = "{{ schedule }}"

with DAG(dag_id, default_args=default_args, schedule=schedule) as dag:

    def emit_last_task_run_time(**context):
        """Emit the logical_date as XCom for dependency tracking."""
        ti = context['ti']
        logical_date = context['logical_date']
        ti.xcom_push(key='last_task_run_time', value=logical_date)

    emit_run_time = PythonOperator(
        task_id='emit_last_task_run_time',
        python_callable=emit_last_task_run_time,
    )

    git_pull = GitPullOperator(task_id='git_pull', dataset=out_dir)

    validate_ddf = ValidateDatasetOperator(
        task_id='validate',
        pool='etl',
        dataset=out_dir,
        logpath=logpath,
    )

    emit_run_time >> git_pull >> validate_ddf
