# -*- coding: utf-8 -*-

import json
import logging
import os.path as osp

from datetime import datetime, timedelta
from pandas import to_datetime

from airflow.exceptions import AirflowSkipException, AirflowException
from airflow.models import Variable, TaskInstance
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.sensors import BaseSensorOperator, ExternalTaskSensor
from airflow.plugins_manager import AirflowPlugin
from airflow.utils.decorators import apply_defaults
from airflow.utils.db import provide_session
from ddf_utils.datapackage import dump_json, get_datapackage
from airflow.utils.state import State


log = logging.getLogger(__name__)


class GenerateDatapackageOperator(PythonOperator):
    def __init__(self, dataset, *args, **kwargs):
        def _gen_dp(d):
            dp = get_datapackage(d, update=True)
            dump_json(osp.join(dataset, 'datapackage.json'), dp)

        super().__init__(python_callable=_gen_dp,
                         op_args=[dataset],
                         *args, **kwargs)


class RunETLOperator(BashOperator):
    def __init__(self, dataset, *args, **kwargs):
        # TODO: think about how to handle datasets_dir here
        bash_command = '''\
        set -eu
        export DATASETS_DIR={{ params.datasets_dir }}
        cd {{ params.dataset }}
        ddf cleanup ddf .

        cd etl/scripts/
        python3 etl.py
        '''
        super().__init__(bash_command=bash_command,
                         params={'dataset': dataset,
                                 'datasets_dir': Variable.get('datasets_dir')},
                         *args, **kwargs)


class UpdateSourceOperator(BashOperator):
    def __init__(self, dataset, *args, **kwargs):
        bash_command = '''\
        set -eu
        export DATASETS_DIR={{ params.datasets_dir }}
        cd {{ params.dataset }}

        cd etl/scripts/
        if [ -f update_source.py ]; then
            python3 update_source.py
            echo "updated source."
        else
            echo "no updater script"
        fi
        '''
        super().__init__(bash_command=bash_command,
                         params={'dataset': dataset,
                                 'datasets_dir': Variable.get('datasets_dir')},
                         *args, **kwargs)


class GitCheckoutOperator(BashOperator):
    def __init__(self, dataset, version, *args, **kwargs):
        bash_command = '''\
        set -eu
        cd {{ params.dataset }}
        git checkout {{ params.version }}
        '''
        super().__init__(bash_command=bash_command,
                         params={'dataset': dataset,
                                 'version': version},
                         *args, **kwargs)


class GitMergeOperator(BashOperator):
    def __init__(self, dataset, head, base, *args, **kwargs):
        bash_command = '''\
        set -eu
        cd {{ params.dataset }}
        git checkout {{ params.base }}
        git merge {{ params.head }}
        '''
        super().__init__(bash_command=bash_command,
                         params={'dataset': dataset,
                                 'head': head,
                                 'base': base},
                         *args, **kwargs)


class GitPushOperator(BashOperator):
    """Check if there are updates, And push when necessary"""
    def __init__(self, dataset, *args, **kwargs):
        bash_command = '''\
        set -eu
        cd {{ params.dataset }}
        if [[ $(git status -s | grep -e '^[? ][?D]' | head -c1 | wc -c) -ne 0 ]]; then
            git add .
            git commit -m "auto generated dataset"
            git push -u origin
        else
            HAS_UPDATE=0
            for f in $(git diff --name-only | grep -v datapackage.json); do
                if [[ $(git diff $f | tail -n +5 | grep -e "^[++|\-\-]" | head -c1 | wc -c) -ne 0 ]]; then
                    HAS_UPDATE=1
                    git add $f
                fi
            done
            if [[ $HAS_UPDATE -eq 1 ]]; then
                git add datapackage.json
                git commit -m "auto generated dataset"
            else
                echo "nothing to push"
            fi
            git reset --hard
            git push -u origin
        fi
        '''
        super().__init__(bash_command=bash_command,
                         params={'dataset': dataset},
                         *args, **kwargs)


class ValidateDatasetOperator(BashOperator):
    def __init__(self, dataset, logpath, *args, **kwargs):
        bash_command = '''\
        set -eu
        cd {{ params.dataset }}
        DT=`date "+%Y-%m-%dT%H-%M-%S"`
        VALIDATE_OUTPUT = "validation-$DT.log"
        echo "logfile: $VALIDATE_OUTPUT"
        validate-ddf ./ --exclude-tags "WARNING TRANSLATION" --silent > $VALIDATE_OUTPUT
        sleep 2
        if [ `cat $VALIDATE_OUTPUT | wc -c` -ge 5 ]
        then
            echo "validation not successful, moving the log file..."
            LOGPATH="{{ params.logpath }}/`basename {{ params.dataset }}`"
            if [ ! -d $LOGPATH ]; then
                mkdir $LOGPATH
            fi
            mv $VALIDATE_OUTPUT $LOGPATH
            exit 1
        else
            echo "validation succeed."
            rm $VALIDATE_OUTPUT
            exit 0
        fi
        '''
        super().__init__(bash_command=bash_command,
                         params={'dataset': dataset,
                                 'logpath': logpath},
                         *args, **kwargs)


class DependencyDatasetSensor(BaseSensorOperator):
    """Sensor that wait for the dependency. If dependency failed, this sensor failed too."""

    @apply_defaults
    def __init__(self, external_dag_id, external_task_id,
                 execution_date=None, allowed_states=[State.SUCCESS], *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.not_allowed_states = [State.FAILED, State.UP_FOR_RETRY, State.UPSTREAM_FAILED]
        self.allowed_states = allowed_states
        self.execution_date = execution_date

        self.external_dag_id = external_dag_id
        self.external_task_id = external_task_id

    @provide_session
    def poke(self, context, session=None):
        if self.execution_date is None:
            dt = context['execution_date']
        else:
            dt = self.execution_date
            if isinstance(dt, str):
                dt = to_datetime(dt)

        dt_start = datetime(dt.year, dt.month, dt.day, 0, 0, 0)
        dt_start = dt_start - timedelta(days=30)
        dt_end = dt_start + timedelta(hours=23, minutes=59, seconds=59)

        log.info(
            'Poking for '
            '{self.external_dag_id}.'
            '{self.external_task_id} on '
            '{} ... '.format(dt.date(), **locals()))
        TI = TaskInstance

        last_tasks = session.query(TI).filter(
            TI.dag_id == self.external_dag_id,
            TI.task_id == self.external_task_id,
            # uncomment below (and modify dt_start/dt_end) if you need to
            # filter the tasks by time.
            # TI.execution_date.between(dt_start, dt_end),
        ).order_by(TI.execution_date.desc())

        last_task = last_tasks.first()

        log.info('task count between {} and {}: {}'.format(dt_start,
                                                           dt_end,
                                                           last_tasks.count()))

        if last_task:
            if last_task.state in self.not_allowed_states:
                raise AirflowException('External task failed.')
            elif last_task.state in self.allowed_states:
                session.commit()
                return True


class DataPackageUpdatedSensor(BaseSensorOperator):
    """Sensor Operation to detect dataset changes."""
    ui_color = '#33ccff'

    @apply_defaults
    def __init__(self, path, dependencies, *args, **kwargs):
        "docstring"
        if not osp.exists(path):
            raise FileNotFoundError('dataset not found: {}'.format(path))
        for p in dependencies:
            if not osp.exists(p):
                raise FileNotFoundError('dataset not found: {}'.format(p))
        self.path = path
        self.dependencies = dependencies
        super().__init__(*args, **kwargs)

    def poke(self, context):
        dp = json.load(open(osp.join(self.path, 'datapackage.json')))
        last_update = dp['last_updated']
        for p in self.dependencies:
            dp_other = json.load(open(osp.join(p, 'datapackage.json')))
            last_update_other = dp_other['last_updated']
            if to_datetime(last_update_other) > to_datetime(last_update):
                self.last_update = last_update
                return True
        raise AirflowSkipException('no need to update')


class LockDataPackageOperator(BaseSensorOperator):
    """Operator to send a xcom variable, to indicator some datasets are required."""
    ui_color = '#666666'

    @apply_defaults
    def __init__(self, op, dps, *args, **kwargs):
        "docstring"
        self.op = op
        self.dps = dps
        self.xcom_key = 'lock_datasets'
        super().__init__(*args, **kwargs)

    def poke(self, context):
        if self.op == 'unlock':
            return True
        xk = self.xcom_key
        locks = self.xcom_pull(context, task_ids=None, key=xk)
        if not isinstance(locks, dict):
            return True
        for d in self.dps:
            if d in locks.keys() and locks[d] is True:
                return False
        return True

    def execute(self, context):
        super().execute(context)
        xk = self.xcom_key
        locks = self.xcom_pull(context, task_ids=None, key=xk)
        if not isinstance(locks, dict):
            locks = {}
        if self.op == 'lock':
            log.info("we will lock:")
            log.info(self.dps)
            for d in self.dps:
                locks[d] = True
            self.xcom_push(context, key=xk, value=locks)
        elif self.op == 'unlock':
            log.info("we will unlock:")
            log.info(self.dps)
            for d in self.dps:
                locks[d] = False
            self.xcom_push(context, key=xk, value=locks)
        else:
            raise ValueError('op should be lock or unlock')


class DDFPlugin(AirflowPlugin):
    name = "ddf plugin"
    operators = [LockDataPackageOperator,
                 DataPackageUpdatedSensor,
                 UpdateSourceOperator,
                 GitCheckoutOperator,
                 GitMergeOperator,
                 GitPushOperator,
                 ValidateDatasetOperator,
                 RunETLOperator,
                 DependencyDatasetSensor,
                 GenerateDatapackageOperator]
