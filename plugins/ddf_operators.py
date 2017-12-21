# -*- coding: utf-8 -*-

import json
import logging
import os.path as osp

from pandas import to_datetime

from airflow.exceptions import AirflowSkipException
from airflow.models import Variable
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.sensors import BaseSensorOperator
from airflow.plugins_manager import AirflowPlugin
from airflow.utils.decorators import apply_defaults
from ddf_utils.datapackage import dump_json, get_datapackage

log = logging.getLogger(__name__)


class GenerateDatapackageOperator(PythonOperator):
    def __init__(self, dataset, *args, **kwargs):
        def _gen_dp(d):
            dp = get_datapackage(d, update=True)
            dump_json(osp.join(dataset, 'datapackage.json'), dp)

        super(GenerateDatapackageOperator, self).__init__(python_callable=_gen_dp,
                                                          op_args=[dataset],
                                                          *args, **kwargs)


class RunETLOperator(BashOperator):
    def __init__(self, dataset, *args, **kwargs):
        # TODO: think about how to handle datasets_dir here
        bash_command = '''\
        set -eu
        export DATASETS_DIR={{ params.datasets_dir }}
        cd {{ params.dataset }}/etl/scripts/
        python etl.py
        '''
        super(RunETLOperator, self).__init__(bash_command=bash_command,
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
        super(GitCheckoutOperator, self).__init__(bash_command=bash_command,
                                                  params={'dataset': dataset,
                                                          'version': version},
                                                  *args, **kwargs)


class GitPushOperator(BashOperator):
    def __init__(self, dataset, *args, **kwargs):
        bash_command = '''\
        set -eu
        cd {{ params.dataset }}
        if [[ $(git diff --name-only | grep -v 'datapackage.json' | head -c1 | wc -c) -ne 0 ]]; then
            git add .
            git commit -m "auto generated dataset"
            git push -u origin
        else
            echo "nothing to push"
            git reset --hard
        fi
        '''
        super(GitPushOperator, self).__init__(bash_command=bash_command,
                                              params={'dataset': dataset},
                                              *args, **kwargs)


class ValidateDatasetOperator(BashOperator):
    def __init__(self, dataset, *args, **kwargs):
        bash_command = '''\
        set -eu
        cd {{ params.dataset }}
        validate-ddf ./
        '''
        super(ValidateDatasetOperator, self).__init__(bash_command=bash_command,
                                                      params={'dataset': dataset},
                                                      *args, **kwargs)


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
        super(DataPackageUpdatedSensor, self).__init__(*args, **kwargs)

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
        super(LockDataPackageOperator, self).__init__(*args, **kwargs)

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
        super(LockDataPackageOperator, self).execute(context)
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
                 GitCheckoutOperator,
                 GitPushOperator,
                 ValidateDatasetOperator,
                 RunETLOperator,
                 GenerateDatapackageOperator]
