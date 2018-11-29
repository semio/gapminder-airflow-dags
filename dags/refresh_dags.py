# -*- coding: utf-8 -*-

import logging
import os.path as osp
import subprocess
from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator
from airflow.exceptions import AirflowSkipException
from jinja2 import Environment, FileSystemLoader

from ddf_utils.chef.api import Chef

logger = logging.getLogger(__name__)

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2018, 3, 15),
    'retry_delay': timedelta(minutes=5),
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    'priority_weight': 200,
    'weight_rule': 'absolute'
    # 'end_date': datetime(2016, 1, 1),
}

datasets_dir = Variable.get('datasets_dir')
airflow_home = Variable.get('airflow_home')
s3_datasets = [x.strip() for x in Variable.get('s3_datasets').split('\n')]

dag = DAG('0-refresh_dags',
          default_args=default_args,
          schedule_interval='@once')


def _get_dataset_type(dataset):
    dataset_path = osp.join(datasets_dir, dataset)
    etl_dir = osp.join(dataset_path, 'etl/scripts')

    out = subprocess.run(['ddf', 'etl_type', '-d', etl_dir],
                         stdout=subprocess.PIPE)
    if out.returncode != 0:
        logging.info('command did not return successfully. fall back to manual')
        return ['manual', '']
    return out.stdout.decode('utf-8').replace('\n', '').split(',')


def _get_denpendencies(dataset, all_datasets, include_indirect=True):
    try:
        etl_type, fn = all_datasets[dataset]
    except KeyError:  # not open_numbers datasets
        return list()

    if etl_type == 'recipe':
        dataset_path = osp.join(datasets_dir, dataset)
        etl_dir = osp.join(dataset_path, 'etl/scripts')
        recipe = osp.join(etl_dir, fn)
        logging.info("using recipe file: " + fn)
        chef = Chef.from_recipe(recipe)
        dependencies = list()
        for i in chef.ingredients:
            if i.ddf_id is not None:
                dependencies.append(i.ddf_id)
                if include_indirect:
                    for d in _get_denpendencies(i.ddf_id, all_datasets):
                        dependencies.append(d)
        dependencies = list(set(dependencies))
        logging.info("dependencies: {}".format(dependencies))
        return dependencies
    else:
        return list()


def refresh_dags(**context):
    """add/modify dags"""
    xcom = context['task_instance'].xcom_pull(task_ids='check_etl_type', key='return_value')
    current = xcom['current_datasets']
    # to_remove = xcom['removal']  # TODO: add code to remove DAG from database.

    env = Environment(loader=FileSystemLoader(osp.join(airflow_home, 'templates')))

    def refresh_normal_dag(dataset):
        # 1. get all dependencies from etl scripts
        # 2. re-generate the DAG, replace the old one
        dependencies = _get_denpendencies(dataset, current)
        etl_type, _ = current[dataset]

        if etl_type == 'recipe':
            now = datetime.utcnow() - timedelta(days=1)
            template = env.get_template('etl_recipe.py')
            p = 100 - len(dependencies)  # The more dependencies, the less priority
        elif etl_type == 'python':
            now = datetime.utcnow() - timedelta(days=7)
            template = env.get_template('etl_recipe.py')
            p = 100
        else:
            now = datetime.utcnow() - timedelta(days=1)
            template = env.get_template('manual_update.py')
            p = 100

        dt_str = 'datetime({}, {}, {})'.format(now.year, now.month, now.day)

        dag_name = dataset.replace('/', '_')
        dag_path = osp.join(airflow_home, 'dags', dag_name)

        # adding dependency checking dags, but don't consider non-open_numbers ones
        direct_deps = _get_denpendencies(dataset, current, include_indirect=False)
        direct_deps = list(filter(lambda x: x.startswith('open-numbers'), direct_deps))
        direct_deps = dict([d, current[d][0]] for d in direct_deps)

        with open(dag_path + '.py', 'w') as f:
            f.write(template.render(name=dataset,
                                    datetime=dt_str,
                                    priority=p,
                                    etl_type=etl_type,
                                    dependencies=direct_deps))
            f.close()

    def refresh_production_dag(dataset):
        now = datetime.utcnow() - timedelta(days=1)
        template = env.get_template('etl_recipe_production.py')
        p = 100
        dt_str = 'datetime({}, {}, {})'.format(now.year, now.month, now.day)

        dag_name = dataset.replace('/', '_') + '_production'
        dag_path = osp.join(airflow_home, 'dags', dag_name)

        with open(dag_path + '.py', 'w') as f:
            f.write(template.render(name=dataset,
                                    datetime=dt_str,
                                    priority=p))
            f.close()

    for ds in current.keys():
        logging.info('checking {}'.format(ds))
        refresh_normal_dag(ds)

        if ds in s3_datasets:
            refresh_production_dag(ds)


refresh_task = PythonOperator(task_id='refresh_dags', dag=dag,
                              provide_context=True,
                              python_callable=refresh_dags)
