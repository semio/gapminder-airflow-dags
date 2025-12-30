# -*- coding: utf-8 -*-

"""{{ name }}"""

import os.path as osp
import subprocess
from datetime import datetime, timedelta

from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.python import BranchPythonOperator, PythonOperator
from airflow.sdk import DAG, Variable

from ddf_operators import (
    GitPullOperator,
    ValidateDatasetOperator,
    create_failure_notification,
)

# steps:
# 1. Pull latest changes
# 2. Check if there were git updates in the past 24 hours
# 3. If yes, validate the dataset; otherwise skip

# variables
target_dataset = '{{ name }}'

datasets_dir = Variable.get('datasets_dir')
airflow_home = Variable.get('airflow_home')
airflow_baseurl = Variable.get('airflow_baseurl')

logpath = osp.join(airflow_home, 'validation-log')
out_dir = osp.join(datasets_dir, target_dataset)
dag_id = target_dataset.replace('/', '_')

# Slack notifications
github_url = f'https://github.com/{target_dataset}'
{% raw %}
log_url = f'{airflow_baseurl}/dags/{dag_id}/runs/{{{{ dag_run.run_id }}}}/tasks/{{{{ ti.task_id }}}}'
{% endraw %}
failure_notification = create_failure_notification(dag_id, github_url, log_url)

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
    'on_failure_callback': [failure_notification],
}

# now define the DAG
schedule = "{{ schedule }}"

with DAG(dag_id, default_args=default_args, schedule=schedule) as dag:

    def emit_last_task_run_time(**context):
        """Emit the logical_date as XCom for dependency tracking."""
        ti = context['ti']
        logical_date = context['logical_date']
        ti.xcom_push(key='last_task_run_time', value=logical_date)

    def check_recent_git_updates(**context):
        """Check if there were git commits in the past 24 hours."""
        result = subprocess.run(
            ['git', 'log', '-1', '--format=%at'],
            cwd=out_dir,
            capture_output=True,
            text=True,
        )
        if result.returncode != 0:
            # If we can't get git info, run validation to be safe
            return 'validate'

        last_commit_timestamp = int(result.stdout.strip())
        yesterday_timestamp = int((datetime.now() - timedelta(days=1)).timestamp())

        if last_commit_timestamp >= yesterday_timestamp:
            return 'validate'
        return 'skip_validation'

    emit_run_time = PythonOperator(
        task_id='emit_last_task_run_time',
        python_callable=emit_last_task_run_time,
    )

    git_pull = GitPullOperator(task_id='git_pull', dataset=out_dir)

    check_updates = BranchPythonOperator(
        task_id='check_recent_updates',
        python_callable=check_recent_git_updates,
    )

    validate_ddf = ValidateDatasetOperator(
        task_id='validate',
        pool='etl',
        dataset=out_dir,
        logpath=logpath,
    )

    skip_validation = EmptyOperator(task_id='skip_validation')

    emit_run_time >> git_pull >> check_updates >> [validate_ddf, skip_validation]
