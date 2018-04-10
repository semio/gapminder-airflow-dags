# -*- coding: utf-8 -*-

"""update all datasets in dataset dir"""

import logging
import os
import os.path as osp
import shutil
import sys
from datetime import datetime, timedelta

import requests
from airflow import DAG
from airflow.models import Variable
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import (BranchPythonOperator,
                                               PythonOperator)
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
    'priority_weight': 99,
    # 'end_date': datetime(2016, 1, 1),
}

datasets_dir = Variable.get('datasets_dir')
airflow_home = Variable.get('airflow_home')
# skipped_datasets = Variable.get('skipped_datasets', deserialize_json=True)

dag = DAG('update_all_datasets',
          default_args=default_args,
          schedule_interval='@daily')

gitpull_template = '''\
set -eu

function git_update() {
    git pull
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

  if [[ $(git branch -a | grep autogenerated | head -c1 | wc -c) -ne 0 ]]
  then
    echo "checkout autogenerated branch"
    git checkout develop
    git_update
    git checkout autogenerated
    git_update
  else
    echo "no autogenerated branch, using master"
    git checkout master
    git_update
  fi
  cd ..
done
'''


def add_remove_datasets():
    """load all datasets from open-numbers, add new datasets and remove closed ones"""
    url = 'https://api.github.com/orgs/open-numbers/repos?per_page=100'
    res = requests.get(url)
    res_json = [res.json()]
    next_link = res.headers.get('Link')

    while next_link is not None:
        url = next_link.split(';')[0][1:-1]
        res = requests.get(url)
        next_link = res.headers.get('Link')
        res_json.append(res.json())

    all_repos = []

    for rs in res_json:
        for r in rs:
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
    to_add = []
    for record in all_repos:
        path = osp.join(datasets_dir, 'open-numbers', record['name'])
        if not osp.exists(path):
            logger.info('cloning dataset: {}'.format(record['name']))
            to_add.append(record['name'])
            os.system('git clone {} {}'.format(record['git_url'].replace('git://', 'git+ssh://git@'),
                                               path))

    datasets_types = dict(['open-numbers/'+k,
                           list(_get_dataset_type('open-numbers/'+k))] for k in current_datasets)

    return {'current_datasets': datasets_types,
            'addition': to_add,
            'removal': list(to_remove)}


def _get_recipe_file(path):
    sys.path.insert(0, path)
    try:
        import etl
        fn = etl.recipe_file
    finally:
        sys.path.remove(path)
        if 'etl' in sys.modules:
            del sys.modules["etl"]
            del etl
    return fn


def _get_dataset_type(dataset):
    dataset_path = osp.join(datasets_dir, dataset)
    etl_dir = osp.join(dataset_path, 'etl/scripts')

    if not osp.exists(etl_dir):
        etl_file = ''
        etl_type = 'manual'
    else:
        try:
            etl_file = _get_recipe_file(etl_dir)
            etl_type = 'recipe'
        except AttributeError:
            etl_file = ''
            etl_type = 'python'
        except ModuleNotFoundError:
            etl_file = ''
            etl_type = 'manual'
        except:
            raise

    return (etl_type, etl_file)


def refresh_dags(**context):
    """add/modify dags"""
    xcom = context['task_instance'].xcom_pull(task_ids='add_remove_datasets', key='return_value')
    current = xcom['current_datasets']
    # to_remove = xcom['removal']  # TODO: add code to remove DAG from database.

    env = Environment(loader=FileSystemLoader(osp.join(airflow_home, 'templates')))

    def refresh_dag(dataset):
        # 1. get all dependencies from etl scripts
        # 2. re-generate the DAG, replace the old one
        dataset_path = osp.join(datasets_dir, dataset)
        etl_dir = osp.join(dataset_path, 'etl/scripts')
        etl_type, fn = current[dataset]

        if etl_type == 'recipe':
            recipe = osp.join(etl_dir, fn)
            logging.info("using recipe file: " + fn)
            chef = Chef.from_recipe(recipe)
            dependencies = set()
            for i in chef.ingredients:
                if i.ddf_id is not None and i.ddf_id.startswith('open-numbers'):
                    dependencies.add(i.ddf_id)
            logging.info("dependencies: {}".format(dependencies))
        else:
            dependencies = set()

        now = datetime.utcnow() - timedelta(days=1)
        dt_str = 'datetime({}, {}, {})'.format(now.year, now.month, now.day)

        if etl_type == 'recipe':
            template = env.get_template('etl_recipe.py')
            p = 1
        elif etl_type == 'python':
            template = env.get_template('etl_recipe.py')
            p = 10
        else:
            template = env.get_template('manual_update.py')
            p = 20

        dag_name = dataset.replace('/', '_')
        dag_path = osp.join(airflow_home, 'dags', dag_name)

        dependencies = dict([d, current[d][0]] for d in dependencies)

        with open(dag_path+'.py', 'w') as f:
            f.write(template.render(name=dataset,
                                    datetime=dt_str,
                                    priority=p,
                                    etl_type=etl_type,
                                    dependencies=dependencies))
            f.close()

    for ds in current.keys():
        logging.info('checking {}'.format(ds))
        refresh_dag(ds)


# 1. set DAG status to pause
# 2. remove the DAG file
# 3. TODO: remove all related info from airflow DB
remove_dag_command = '''\
set -eu

cd {{ params.dags_dir }}

{% for dag_id in task_instance.xcom_pull(
    task_ids='add_remove_datasets', key='return_value')['removal'] %}\
airflow pause open-numbers_{{ dag_id }};
rm {{ dag_id }}.py;
{% endfor %}

'''

review_task = PythonOperator(task_id='add_remove_datasets',
                             dag=dag,
                             python_callable=add_remove_datasets)

refresh_task = PythonOperator(task_id='refresh_dags', dag=dag,
                              provide_context=True,
                              python_callable=refresh_dags)

remove_task = BashOperator(task_id='remove_dags', dag=dag,
                           bash_command=remove_dag_command,
                           params={'dags_dir': osp.join(airflow_home, 'dags')})

git_pull_task = BashOperator(task_id='update_all_dataset',
                             bash_command=gitpull_template,
                             params={'datasetpath': datasets_dir},
                             retries=3,
                             retry_delay=timedelta(seconds=10),
                             dag=dag)

review_task >> git_pull_task >> refresh_task >> remove_task
