# -*- coding: utf-8 -*-

import json
import logging
import os.path as osp

from datetime import datetime, timedelta
from pandas import to_datetime

from urllib.parse import urlencode, urljoin

from ddf_utils.io import dump_json
from ddf_utils.package import get_datapackage

from airflow.exceptions import AirflowSkipException, AirflowException
from airflow.models import Variable, TaskInstance
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.sensors.base_sensor_operator import BaseSensorOperator
from airflow.operators.http_operator import SimpleHttpOperator
from airflow.plugins_manager import AirflowPlugin
from airflow.utils.decorators import apply_defaults
from airflow.utils.state import State
from airflow.utils.db import provide_session


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
        ddf cleanup --exclude icon.png ddf .

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
    def __init__(self, dataset, branch, *args, **kwargs):
        bash_command = '''\
        set -eu
        cd {{ params.dataset }}
        git checkout {{ params.branch }}
        git pull
        '''
        super().__init__(bash_command=bash_command,
                         params={'dataset': dataset,
                                 'branch': branch},
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
            echo "git updated."
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
                git push -u origin
                echo "git updated."
            else
                echo "nothing to push"
            fi
        fi
        '''
        super().__init__(bash_command=bash_command,
                         params={'dataset': dataset},
                         *args, **kwargs)


class GitResetOperator(BashOperator):
    def __init__(self, dataset, *args, **kwargs):
        bash_command = '''\
        set -eu
        cd {{ params.dataset }}
        git reset --hard
        git clean -dfx
        '''
        super().__init__(bash_command=bash_command,
                         params={'dataset': dataset},
                         *args, **kwargs)


class ValidateDatasetOperator(BashOperator):
    def __init__(self, dataset, logpath, *args, **kwargs):
        bash_command = '''\
        cd {{ params.dataset }}
        DT=`date "+%Y-%m-%dT%H-%M-%S"`
        VALIDATE_OUTPUT="validation-$DT.log"
        echo "logfile: $VALIDATE_OUTPUT"
        RES=`validate-ddf ./ --exclude-tags "WARNING TRANSLATION" --silent --heap 8192 --multithread`
        if [ $? -eq 0 ]
        then
            sleep 2
            echo "validation succeed."
            exit 0
        else
            sleep 2
            echo $RES > $VALIDATE_OUTPUT
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
                echo "ddf-validation failed but no output."
                rm $VALIDATE_OUTPUT
                exit 1
            fi
        fi
        '''
        super().__init__(bash_command=bash_command,
                         params={'dataset': dataset,
                                 'logpath': logpath},
                         *args, **kwargs)


class S3UploadOperator(BashOperator):
    """upload a dataset to the target bucket. We don't use S3 Hook because it doesn't support uploading directory yet."""
    def __init__(self, dataset, branch, bucket, *args, **kwargs):
        bash_command = '''\
        set -eu
        cd {{ params.dataset }}
        git checkout {{ params.branch }}
        aws s3 sync . {{ params.bucket_path }} --delete
        '''

        super().__init__(bash_command=bash_command,
                         params={'dataset': dataset,
                                 'branch': branch,
                                 'bucket_path': bucket},
                         *args, **kwargs)


class GCSUploadOperator(BashOperator):
    """upload a dataset to the target bucket."""
    def __init__(self, dataset, branch, bucket, *args, **kwargs):
        bash_command = '''\
        set -eu
        cd {{ params.dataset }}
        git checkout {{ params.branch }}
        gsutil -h "Cache-Control: no-cache, no-store, must-revalidate" -m rsync -d -r -j csv -x '\.git.*$|etl/.*$' . "{{ params.bucket_path }}"
        '''

        super().__init__(bash_command=bash_command,
                         params={'dataset': dataset,
                                 'branch': branch,
                                 'bucket_path': bucket},
                         *args, **kwargs)


class CleanCFCacheOperator(BashOperator):
    """clean cloudflare cache for a zone and cache tag."""
    def __init__(self, zone_id, cache_tags=None, *args, **kwargs):
        if cache_tags:
            bash_command = '''\
            set -eu
            cli4 --delete tags={{ params.cache_tags }} /zones/:{{ params.zone_id }}/purge_cache
            '''
        else:
            bash_command = '''\
            set -eu
            cli4 --delete purge_everything=true /zones/:{{ params.zone_id }}/purge_cache
            '''
        super().__init__(bash_command=bash_command,
                         params={'cache_tags': cache_tags,
                                 'zone_id': zone_id},
                         *args, **kwargs)


class ValidateDatasetDependOnGitOperator(BashOperator):
    def __init__(self, dataset, logpath, *args, **kwargs):
        bash_command = '''\
        cd {{ params.dataset }}
        LASTGITCOMMITDATE=`git log -1 --format=%at`
        YESTERDAY=`date -d "yesterday" "+%s"`

        run () {
            DT=`date "+%Y-%m-%dT%H-%M-%S"`
            VALIDATE_OUTPUT="validation-$DT.log"
            echo "logfile: $VALIDATE_OUTPUT"
            RES=`validate-ddf ./ --exclude-tags "WARNING TRANSLATION" --silent --heap 8192 --multithread`
            if [ $? -eq 0 ]
            then
                sleep 2
                echo "validation succeed."
                exit 0
            else
                sleep 2
                echo $RES > $VALIDATE_OUTPUT
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
                    echo "ddf-validation failed but no output."
                    rm $VALIDATE_OUTPUT
                    exit 1
                fi
            fi
        }

        if [ $LASTGITCOMMITDATE -ge $YESTERDAY ]
        then
            echo "there is new updates, need to validate"
            run
        else
            echo "no updates."
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
                 execution_date=None,
                 allowed_states=[State.SUCCESS],
                 not_allowed_states=[State.FAILED, State.UP_FOR_RETRY, State.UPSTREAM_FAILED],
                 *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.not_allowed_states = not_allowed_states
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

        dt_today = datetime(dt.year, dt.month, dt.day, 0, 0, 0)
        dt_start = dt_today - timedelta(days=30)                          # check tasks between 30 days ago
        dt_end = dt_today + timedelta(hours=23, minutes=59, seconds=59)  # and the end of today.

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

        # log.info('task count between {} and {}: {}'.format(dt_start,
        #                                                    dt_end,
        #                                                    last_tasks.count()))
        log.info("last task instance was executed on {}".format(last_task.execution_date))

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


class SlackReportOperator(SimpleHttpOperator):
    """Operator to report a message to slack with default buttons"""

    @apply_defaults
    def __init__(self, status, airflow_baseurl, *args, **kwargs):
        """report status of task in slack.

        status: task status, possible values are same as task status in airflow
        """
        super(SlackReportOperator, self).__init__(*args, **kwargs)
        self.status = status
        self.airflow_baseurl = airflow_baseurl

    def execute(self, context):
        # overwrite self.data to custom message
        dag_id = context['dag_run'].dag_id
        task_id = context['ti'].task_id
        ts = context['ts']
        dataset = context.get('target_dataset', None)

        text = f"{dag_id}.{task_id}: {self.status}"
        log_url = osp.join(self.airflow_baseurl, 'admin/airflow/log?')
        log_url = log_url + urlencode({'task_id': task_id, 'dag_id': dag_id, 'execution_date': ts, 'format': 'json'})

        blocks = [
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": text
                }
            },
            {
                "type": "actions",
                "elements": [
                    {
                        "type": "button",
                        "text": {
                            "type": "plain_text",
                            "text": "Show log"
                        },
                        "url": log_url
                    }
                ]
            }
        ]

        if dataset:
            git_url = urljoin("https://github.com", dataset)
            blocks[1]['elements'].append(
                {
                    "type": "button",
                    "text": {
                        "type": "plain_text",
                        "text": "Github"
                    },
                    "url": git_url
                }
            )

        data = {"blocks": blocks}
        data = json.dumps(data)

        self.data = data
        self.log.info(data)
        super().execute(context)


class DDFPlugin(AirflowPlugin):
    name = "ddf_plugin"
    operators = [LockDataPackageOperator,
                 UpdateSourceOperator,
                 GitCheckoutOperator,
                 GitMergeOperator,
                 GitPushOperator,
                 GitResetOperator,
                 CleanCFCacheOperator,
                 ValidateDatasetOperator,
                 ValidateDatasetDependOnGitOperator,
                 S3UploadOperator,
                 GCSUploadOperator,
                 RunETLOperator,
                 GenerateDatapackageOperator,
                 SlackReportOperator]
    sensors = [DataPackageUpdatedSensor,
               DependencyDatasetSensor]
