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
                                          GitResetOperator, CleanCFCacheOperator,
                                          GCSUploadOperator, ValidateDatasetOperator)
from airflow.operators.subdag_operator import SubDagOperator
from airflow.executors.local_executor import LocalExecutor

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
    'execution_timeout': timedelta(hours=10),     # 10 hours
    'weight_rule': 'absolute'
}

target_dataset = '{{ name }}'
depends_on = {{ dependencies }}

# variables
datasets_dir = Variable.get('datasets_dir')
airflow_home = Variable.get('airflow_home')
s3_datasets = [x.strip() for x in Variable.get('s3_datasets').split('\n')]

logpath = osp.join(airflow_home, 'validation-log')
out_dir = osp.join(datasets_dir, target_dataset)
dag_id = target_dataset.replace('/', '_')
sub_dag_id = dag_id + '.' + 'dependency_check'

# now define the DAG
etl_type = "{{ etl_type }}"

{% if etl_type == 'recipe' %}
schedule = '0 12 * * *'   # recipe datasets: 12:00 everyday
{% else %}
schedule = '0 1 * * 0'    # source datasets: 1:00 every Sunday
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
        'timeout': 60 * 60 * 8,
        'priority_weight': {{ priority }},
        'weight_rule': 'absolute',
        'pool': 'dependency_checking'
    }
    subdag = DAG(sub_dag_id, default_args=args, schedule_interval='@once')

    dep_tasks = []

    if etl_type != 'recipe':
        update_datasets = DependencyDatasetSensor(task_id='update_datasets', dag=subdag,
                                                  external_dag_id='update_all_datasets',
                                                  external_task_id='refresh_dags')

    for dep in depends_on.keys():
        t = DependencyDatasetSensor(task_id='wait_for_{}'.format(dep).replace('/', '_'),
                                    dag=subdag,
                                    external_dag_id=dep.replace('/', '_'),
                                    external_task_id='validate')
        dep_tasks.append(t)

    return subdag


dependency_task = SubDagOperator(subdag=sub_dag(), task_id='dependency_check', dag=dag, executor=LocalExecutor(parallelism=2))

checkout_task = GitCheckoutOperator(task_id='checkout_autogen_branch', dag=dag,
                                    dataset=out_dir, branch='autogenerated')
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
# reseting the branch in case of anything failed
cleanup_task = GitResetOperator(task_id='cleanup', dag=dag, dataset=out_dir, trigger_rule="all_done")

# invalidate cloudflare cache
clean_cf_cache = CleanCFCacheOperator(dag=dag, task_id='clean_cf_cache', zone_id='gapminderdev.org')

# set dependencies
(dependency_task >>
 checkout_task >>
 source_update_task >>
 recipe_task >>
 datapackage_task >>
 validate_ddf >>
 git_push_task)


# TODO: rename s3 to gcs
if target_dataset in s3_datasets:
    def gcs_subdag():
        subdag_id = dag_id + '.' + 'upload_to_GCS'
        subdag = DAG(subdag_id, default_args=default_args, schedule_interval='@once')
        # uploading to prod
        bucket = f"gs://gapminder-ws-prod-ds-storage/{target_dataset}/autogenerated/head/"
        gcs_upload_prod = GCSUploadOperator(dag=subdag, task_id='prod', dataset=out_dir,
                                            branch='autogenerated', bucket=bucket)
        # uploading to dev
        bucket = f"gs://gapminder-ws-dev-ds-storage/{target_dataset}/autogenerated/head/"
        gcs_upload_dev = GCSUploadOperator(dag=subdag, task_id='dev', dataset=out_dir,
                                           branch='autogenerated', bucket=bucket)
        return subdag

    gcs_upload = SubDagOperator(subdag=gcs_subdag(), task_id='upload_to_GCS', dag=dag, executor=LocalExecutor(parallelism=1))

    (git_push_task >> gcs_upload >> clean_cf_cache >> cleanup_task)
else:
    (git_push_task >> clean_df_cache >> cleanup_task)
