# -*- coding: utf-8 -*-

"""update all datasets in dataset dir"""

import logging
import os
import os.path as osp
import shutil
import sys
import subprocess
from datetime import datetime, timedelta

import requests
from airflow import DAG
from airflow.models import Variable
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import (BranchPythonOperator,
                                               PythonOperator)
from airflow.exceptions import AirflowSkipException, DagNotFound
from airflow.api.client.local_client import Client
from jinja2 import Environment, FileSystemLoader

from ddf_utils.chef.api import Chef

logger = logging.getLogger(__name__)

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2018, 3, 15),
    'retry_delay': timedelta(minutes=5),
    'priority_weight': 200,
    'weight_rule': 'absolute',
    'catchup': False
}

datasets_dir = Variable.get('datasets_dir')
airflow_home = Variable.get('airflow_home')
gcs_datasets = [x.strip() for x in Variable.get('with_production').split('\n')]
auto_datasets = [x.strip() for x in Variable.get('automatic_datasets').split('\n')]
auto_ws_datasets = [x.strip() for x in Variable.get('automatic_ws_datasets').split('\n')]
custom_schedule = Variable.get('custom_schedule', deserialize_json=True)

dag = DAG('update_all_datasets',
          default_args=default_args,
          schedule_interval='@daily')

gitpull_template = '''\
set -eu

function git_update_submodule() {
    git submodule init
    git submodule sync
    git submodule update --recursive --remote
}

DIR={{ params.datasetpath }}

cd $DIR/open-numbers

for i in `ls`; do
  cd "$i"
  echo "updating: $i"
  git reset --hard
  git clean -fdx
  git pull
  git remote prune origin

  echo "updating master branch"
  if [[ $(git branch -a | grep master | head -c1 | wc -c) -ne 0 ]]
  then
      git checkout master
      git reset --hard origin/master
      git_update_submodule

      if [[ $(git branch -a | grep origin/autogenerated | head -c1 | wc -c) -ne 0 ]]
      then
          echo "updating autogenerated branch"
          git checkout autogenerated
          git reset --hard origin/autogenerated
          git_update_submodule
      fi
      if [[ $(git branch -a | grep origin/develop | head -c1 | wc -c) -ne 0 ]]
      then
          echo "updating develop branch"
          git checkout develop
          git reset --hard origin/develop
          git_update_submodule
      fi
  else
      echo "no master branch, skipping"
  fi
  cd ..
done
'''

git_checkmaster_template = '''\
set -eu

DIR={{ params.datasetpath }}

cd $DIR/open-numbers

for i in `ls`; do
  cd "$i"
  echo "updating: $i"

  if [[ $(git branch -a | grep master | head -c1 | wc -c) -ne 0 ]]
  then
      git checkout master
  fi
  cd ..
done

'''


def add_remove_datasets():
    """load all datasets from open-numbers, add new datasets and remove closed ones"""
    url = 'https://api.github.com/orgs/open-numbers/repos?per_page=100'
    res = requests.get(url)
    res_json = [res.json()]
    next_link = res.links.get('next')

    while next_link is not None:
        url = next_link['url']
        res = requests.get(url)
        next_link = res.links.get('next')
        res_json.append(res.json())

    all_repos = []

    for rs in res_json:
        for r in rs:
            if type(r) != dict:
                print(f"skipped record because it's not a dictionary: {r}")
                continue
            if not r['archived']:
                all_repos.append(r)

    # remove datasets
    existing_datasets = []
    for i in os.listdir(osp.join(datasets_dir, 'open-numbers')):
        if osp.isdir(osp.join(datasets_dir, 'open-numbers', i)):
            existing_datasets.append(i)
    current_datasets = [x['name'] for x in all_repos]

    to_remove = set(existing_datasets) - set(current_datasets)
    for record in to_remove:
        logger.info('removing dataset: {}'.format(record))
        path = osp.join(datasets_dir, 'open-numbers', record)
        shutil.rmtree(path)

    # add datasets
    for record in all_repos:
        path = osp.join(datasets_dir, 'open-numbers', record['name'])
        if not osp.exists(path):
            logger.info('cloning dataset: {}'.format(record['name']))
            # to_add.append(record['name'])
            os.system('git clone {} {}'.format(record['git_url'].replace('git://', 'git+ssh://git@'),
                                               path))
    # return datasets that need to be removed from airflow DB
    return list(to_remove)


def check_etl_type():
    current_datasets = os.listdir(osp.join(datasets_dir, 'open-numbers'))
    datasets_types = dict(['open-numbers/' + k,
                           list(_get_dataset_type('open-numbers/' + k))] for k in current_datasets)

    return {'current_datasets': datasets_types}


def _get_dataset_type(dataset):
    dataset_path = osp.join(datasets_dir, dataset)
    etl_dir = osp.join(dataset_path, 'etl/scripts')

    out = subprocess.run(['ddf', 'etl_type', '-d', etl_dir],
                         stdout=subprocess.PIPE)
    if out.returncode != 0:
        logging.info('command did not return successfully. fall back to manual')
        return ['manual', '']
    return out.stdout.decode('utf-8').split('\n')[-2].split(',')


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
        chef = Chef.from_recipe(recipe, ddf_dir=datasets_dir)
        dependencies = list()
        for i in chef.ingredients:
            if i.dataset is not None:
                dependencies.append(i.dataset)
                if include_indirect:
                    for d in _get_denpendencies(i.dataset, all_datasets):
                        dependencies.append(d)
        dependencies = list(set(dependencies))
        logging.info("dependencies: {}".format(dependencies))
        return dependencies
    else:
        return list()


def refresh_dags(**context):
    """add/modify/remove dags"""
    api_client = Client(api_base_url=None, auth=None)
    xcom_current = context['task_instance'].xcom_pull(task_ids='check_etl_type', key='return_value')
    current = xcom_current['current_datasets']
    remove = context['task_instance'].xcom_pull(task_ids='add_remove_datasets', key='return_value')
    env = Environment(loader=FileSystemLoader(osp.join(airflow_home, 'templates')))

    def refresh_normal_dag(dataset):
        # 1. get all dependencies from etl scripts
        # 2. re-generate the DAG, replace the old one
        dependencies = _get_denpendencies(dataset, current)
        etl_type, _ = current[dataset]

        if etl_type == 'recipe':
            now = datetime.utcnow() - timedelta(days=1)
            if dataset in auto_ws_datasets:
                template = env.get_template('etl_recipe_auto_ws.py')
            elif dataset in auto_datasets:
                template = env.get_template('etl_recipe_auto.py')
            else:
                template = env.get_template('etl_recipe.py')
            p = 100 - len(dependencies)  # The more dependencies, the less priority
        elif etl_type == 'python':
            now = datetime.utcnow() - timedelta(days=7)
            if dataset in auto_ws_datasets:
                template = env.get_template('etl_recipe_auto_ws.py')
            elif dataset in auto_datasets:
                template = env.get_template('etl_recipe_auto.py')
            else:
                template = env.get_template('etl_recipe.py')
            p = 100
        else:
            now = datetime.utcnow() - timedelta(days=1)
            template = env.get_template('manual_update.py')
            p = 100

        # config schedule
        schedule = custom_schedule.get(dataset, None)
        if not schedule:
            if etl_type == 'recipe':
                schedule = '0 12 * * *'   # recipe datasets: 12:00 everyday
            elif etl_type == 'python':
                schedule = '0 1 * * 0'    # source datasets: 1:00 every Sunday
            else:
                schedule = '30 0 * * *'   # manual datasets: 0:30 everyday

        dt_str = 'datetime({}, {}, {})'.format(now.year, now.month, now.day)

        dag_name = dataset.replace('/', '_')
        dag_path = osp.join(airflow_home, 'dags', 'datasets', dag_name)

        # adding dependency checking dags, but don't consider non-open_numbers ones
        direct_deps = _get_denpendencies(dataset, current, include_indirect=False)
        direct_deps = list(filter(lambda x: x.startswith('open-numbers'), direct_deps))
        direct_deps = dict([d, current[d][0]] for d in direct_deps)

        with open(dag_path + '.py', 'w') as f:
            f.write(template.render(name=dataset,
                                    schedule=schedule,
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
        dag_path = osp.join(airflow_home, 'dags', 'datasets', dag_name)

        with open(dag_path + '.py', 'w') as f:
            f.write(template.render(name=dataset,
                                    datetime=dt_str,
                                    priority=p))
            f.close()

    def remove_dataset_from_db(dataset):
        dag_name = dataset.replace('/', '_')
        try:
            api_client.delete_dag(dag_id=dag_name)
        except DagNotFound:
            # delete_dag will do below if the dag exists:
            # return f"Removed {count} record(s)"
            return "Removed 0 records"

    for ds in current.keys():
        logging.info('checking {}'.format(ds))
        refresh_normal_dag(ds)

        if ds in gcs_datasets:
            refresh_production_dag(ds)

    for ds in remove:
        logging.info('removing {} from Database'.format(ds))
        remove_dataset_from_db(ds)


# command to remove all dags in a dir.
remove_dag_command = '''\
set -eu

cd {{ params.dags_dir }}

rm ./*.py

'''

review_task = PythonOperator(task_id='add_remove_datasets',
                             dag=dag,
                             python_callable=add_remove_datasets)

remove_task = BashOperator(task_id='remove_dags', dag=dag,
                           bash_command=remove_dag_command,
                           params={'dags_dir': osp.join(airflow_home, 'dags', 'datasets')})

refresh_task = PythonOperator(task_id='refresh_dags', dag=dag,
                              provide_context=True,
                              python_callable=refresh_dags)

git_pull_task = BashOperator(task_id='pull_branches',
                             bash_command=gitpull_template,
                             params={'datasetpath': datasets_dir},
                             retries=3,
                             retry_delay=timedelta(seconds=10),
                             dag=dag)

git_checkout_task = BashOperator(task_id='checkout_master_branches',
                             bash_command=git_checkmaster_template,
                             params={'datasetpath': datasets_dir},
                             retries=3,
                             retry_delay=timedelta(seconds=10),
                             trigger_rule="all_done",
                             dag=dag)

check_etl_type_task = PythonOperator(task_id='check_etl_type', dag=dag,
                                     python_callable=check_etl_type)

# define the DAG
(
    review_task >>
    git_pull_task >>
    check_etl_type_task >>
    remove_task >>
    refresh_task >>
    git_checkout_task
)
