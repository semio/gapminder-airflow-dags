# -*- coding: utf-8 -*-

"""{{ name }}"""

import os
import os.path as osp
from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.hooks.base_hook import BaseHook
from ddf_operators import (GenerateDatapackageOperator,
                           DependencyDatasetSensor,
                           UpdateSourceOperator, GitCommitOperator,
                           GitCheckoutOperator, GitPushOperator,
                           GitMergeOperator, RunETLOperator,
                           GitResetAndGoMasterOperator, CleanCFCacheOperator,
                           GCSUploadOperator, ValidateDatasetOperator,
                           SlackReportOperator, GitPullOperator,
                           NotifyWaffleServerOperator)
from airflow.operators.subdag_operator import SubDagOperator
from airflow.operators.python_operator import BranchPythonOperator
from airflow.executors.local_executor import LocalExecutor
from airflow.operators.dummy_operator import DummyOperator

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


# variables
datasets_dir = Variable.get('datasets_dir')
airflow_home = Variable.get('airflow_home')
# gcs_datasets = [x.strip() for x in Variable.get('with_production').split('\n')]
endpoint = BaseHook.get_connection('slack_connection').password
airflow_baseurl = BaseHook.get_connection('airflow_web').host

target_dataset = '{{ name }}'
depends_on = {{ dependencies }}

logpath = osp.join(airflow_home, 'validation-log')
out_dir = osp.join(datasets_dir, target_dataset)
dag_id = target_dataset.replace('/', '_')
sub_dag_id = dag_id + '.' + 'dependency_check'


def slack_report(context, status):
    task = SlackReportOperator(task_id='slack_report', http_conn_id='slack_connection',
                               endpoint=endpoint, status=status, airflow_baseurl=airflow_baseurl)
    context['target_dataset'] = '{{ name }}'
    task.execute(context)


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
    'weight_rule': 'absolute',
    'on_failure_callback': partial(slack_report, status='failed')
}

# now define the DAG
etl_type = "{{ etl_type }}"

schedule = "{{ schedule }}"

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

    dep_tasks = []

    # if etl_type != 'recipe':
    #     update_datasets = DependencyDatasetSensor(task_id='update_datasets', dag=subdag,
    #                                               external_dag_id='update_all_datasets',
    #                                               external_task_id='refresh_dags')

    for dep, dep_etl_type in depends_on.items():
        if dep_etl_type == 'manual':
            t = DependencyDatasetSensor(task_id='wait_for_{}'.format(dep).replace('/', '_'),
                                        dag=subdag,
                                        external_dag_id=dep.replace('/', '_'),
                                        external_task_id='validate')
        else:
            t = DependencyDatasetSensor(task_id='wait_for_{}'.format(dep).replace('/', '_'),
                                        dag=subdag,
                                        external_dag_id=dep.replace('/', '_'),
                                        external_task_id='cleanup')
        dep_tasks.append(t)

    return subdag


checkout_task = GitCheckoutOperator(task_id='checkout_autogen_branch', dag=dag,
                                    dataset=out_dir, branch='autogenerated')
git_pull_task = GitPullOperator(task_id='git_pull', dag=dag, dataset=out_dir)
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


git_commit_task = GitCommitOperator(task_id='git_commit', dag=dag,
                                    pool='etl', dataset=out_dir)


def check_new_commit(**kwargs):
    ti = kwargs['ti']
    dag_id = ti.dag_id
    res = ti.xcom_pull(task_ids='git_commit', dag_id=dag_id)
    if "git updated" in res:
        return "merge_into_master"
    return "do_nothing"


branch_task = BranchPythonOperator(task_id='check_new_commit', dag=dag,
                                   python_callable=check_new_commit, provide_context=True)


{% if name == 'open-numbers/ddf--gapminder--systema_globalis' %}
git_merge_task = GitMergeOperator(task_id='merge_into_master', dag=dag,
                                  dataset=out_dir, base='develop', head='autogenerated')
{% else %}
git_merge_task = GitMergeOperator(task_id='merge_into_master', dag=dag,
                                  dataset=out_dir, base='master', head='autogenerated')
{% endif %}


def git_push_callback(context):
    slack_report(context, status='new data')


git_push_task = GitPushOperator(task_id='git_push', dag=dag,
                                pool='etl',
                                dataset=out_dir,
                                push_all=True,
                                on_success_callback=git_push_callback)

notify_ws_task = NotifyWaffleServerOperator(task_id='notify_waffle_server', dag=dag, dataset=target_dataset)

# reseting the branch in case of anything failed
cleanup_task = GitResetAndGoMasterOperator(task_id='cleanup', dag=dag, dataset=out_dir, trigger_rule="all_done")


# set dependencies
if len(depends_on) > 0:
    dependency_task = SubDagOperator(subdag=sub_dag(), task_id='dependency_check', on_failure_callback=None,
                                     dag=dag)
    dependency_task >> checkout_task

# etl
(checkout_task >>
 git_pull_task >>
 source_update_task >>
 recipe_task >>
 datapackage_task >>
 validate_ddf >>
 git_commit_task)

# commit
do_nothing = DummyOperator(task_id='do_nothing', dag=dag)
git_commit_task >> branch_task
branch_task >> git_merge_task >> git_push_task >>  notify_ws_task >> cleanup_task
branch_task >> do_nothing >> cleanup_task
