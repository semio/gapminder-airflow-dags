# -*- coding: utf-8 -*-

import logging

from airflow.providers.slack.hooks.slack_webhook import SlackWebhookHook
from airflow.providers.slack.notifications.slack_webhook import (
    send_slack_webhook_notification,
)
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.sensors.external_task import ExternalTaskSensor
from airflow.sdk import Variable

log = logging.getLogger(__name__)


# =============================================================================
# Slack Notification Helpers
# =============================================================================


def _create_slack_blocks(message: str, github_url: str, log_url: str) -> list[dict]:
    """Create Slack Block Kit structure with message and buttons."""
    return [
        {"type": "section", "text": {"type": "mrkdwn", "text": message}},
        {
            "type": "actions",
            "elements": [
                {
                    "type": "button",
                    "text": {"type": "plain_text", "text": "GitHub"},
                    "url": github_url,
                },
                {
                    "type": "button",
                    "text": {"type": "plain_text", "text": "Logs"},
                    "url": log_url,
                },
            ],
        },
    ]


def create_failure_notification(dag_id: str, github_url: str, log_url_template: str):
    """Create a Slack failure notification with Block Kit buttons.

    Args:
        dag_id: The DAG ID for the notification message
        github_url: URL to the GitHub repository
        log_url_template: URL template for logs (can contain Jinja2 {{ }} placeholders)

    Returns:
        A configured send_slack_webhook_notification callable
    """
    return send_slack_webhook_notification(
        slack_webhook_conn_id="slack_webhook",
        text=f"{dag_id}.{{{{ ti.task_id }}}}: failed",
        blocks=_create_slack_blocks(
            message=f"*{dag_id}.{{{{ ti.task_id }}}}*: failed :x:",
            github_url=github_url,
            log_url=log_url_template,
        ),
    )


def create_success_notification(dag_id: str, github_url: str, log_url_template: str):
    """Create a Slack success notification with Block Kit buttons.

    Args:
        dag_id: The DAG ID for the notification message
        github_url: URL to the GitHub repository
        log_url_template: URL template for logs (can contain Jinja2 {{ }} placeholders)

    Returns:
        A configured send_slack_webhook_notification callable
    """
    return send_slack_webhook_notification(
        slack_webhook_conn_id="slack_webhook",
        text=f"{dag_id}.{{{{ ti.task_id }}}}: new data",
        blocks=_create_slack_blocks(
            message=f"*{dag_id}.{{{{ ti.task_id }}}}*: new data :white_check_mark:",
            github_url=github_url,
            log_url=log_url_template,
        ),
    )


def create_dependency_failure_callback(
    dag_id: str, github_url: str, airflow_baseurl: str
):
    """Create a callback for dependency failures that sends only one notification per DAG run.

    Uses XCom to track if a notification was already sent for this DAG run,
    preventing duplicate notifications when multiple dependencies fail.

    Args:
        dag_id: The DAG ID for the notification message
        github_url: URL to the GitHub repository
        airflow_baseurl: Base URL of the Airflow webserver

    Returns:
        A callback function suitable for on_failure_callback
    """

    def _callback(context):
        ti = context["ti"]
        dag_run = context["dag_run"]

        # Check if we already sent a notification for this DAG run
        notified = ti.xcom_pull(key="dependency_failure_notified", dag_id=dag_id)
        if notified:
            return

        # Mark as notified
        ti.xcom_push(key="dependency_failure_notified", value=True)

        # Build log URL
        log_url = (
            f"{airflow_baseurl}/dags/{dag_id}/runs/{dag_run.run_id}/tasks/{ti.task_id}"
        )

        # Send notification
        hook = SlackWebhookHook(slack_webhook_conn_id="slack_webhook")
        hook.send(
            text=f"{dag_id}: dependency check failed",
            blocks=_create_slack_blocks(
                message=f"*{dag_id}*: dependency check failed :x:\nFailed task: {ti.task_id}",
                github_url=github_url,
                log_url=log_url,
            ),
        )

    return _callback


class SetupVenvOperator(BashOperator):
    """Create and setup a Python virtual environment for ETL scripts.

    Uses uv to create a venv in the etl/ folder and installs dependencies.
    If etl/requirements.txt exists, installs from it; otherwise installs ddf_utils.
    """

    def __init__(self, dataset, *args, **kwargs):
        bash_command = """\
        set -eu
        cd {{ params.dataset }}/etl

        # Create venv using uv and activate it
        uv venv .venv
        source .venv/bin/activate

        # Install dependencies
        if [ -f requirements.txt ]; then
            echo "Installing from requirements.txt"
            uv pip install -r requirements.txt
        else
            echo "No requirements.txt found, installing ddf_utils"
            uv pip install ddf_utils
        fi
        """
        super().__init__(
            bash_command=bash_command,
            params={"dataset": dataset},
            *args,
            **kwargs,
        )


class GenerateDatapackageOperator(BashOperator):
    """Generate datapackage.json and validate the dataset using validate-ddf-ng.

    Uses the -p flag to generate datapackage.json while validating.
    """

    def __init__(self, dataset, *args, **kwargs):
        bash_command = """\
        export NODE_OPTIONS="--max-old-space-size=7000"
        cd {{ params.dataset }}
        validate-ddf-ng -p --no-warning ./
        if [ $? -eq 0 ]
        then
            sleep 2
            echo "validation and datapackage generation succeed."
            exit 0
        else
            sleep 2
            echo "validation failed."
            exit 1
        fi
        """
        super().__init__(
            bash_command=bash_command,
            params={"dataset": dataset},
            *args,
            **kwargs,
        )


class RunETLOperator(BashOperator):
    def __init__(self, dataset, *args, **kwargs):
        bash_command = """\
        set -eu
        export DATASETS_DIR={{ params.datasets_dir }}
        cd {{ params.dataset }}
        # Activate venv and run ETL
        source etl/.venv/bin/activate
        
        # cleanup all files.
        ddf cleanup --exclude icon.png ddf .

        cd etl/scripts/
        python etl.py
        """
        super().__init__(
            bash_command=bash_command,
            params={"dataset": dataset, "datasets_dir": Variable.get("datasets_dir")},
            env={
                "GSPREAD_PANDAS_CONFIG_DIR": Variable.get("GSPREAD_PANDAS_CONFIG_DIR")
            },
            *args,
            **kwargs,
        )


class UpdateSourceOperator(BashOperator):
    def __init__(self, dataset, *args, **kwargs):
        bash_command = """\
        set -eu
        export DATASETS_DIR={{ params.datasets_dir }}
        cd {{ params.dataset }}

        # Activate venv and run update_source if it exists
        source etl/.venv/bin/activate
        cd etl/scripts/
        if [ -f update_source.py ]; then
            python update_source.py
            echo "updated source."
        else
            echo "no updater script"
        fi
        """
        super().__init__(
            bash_command=bash_command,
            params={"dataset": dataset, "datasets_dir": Variable.get("datasets_dir")},
            env={
                "GSPREAD_PANDAS_CONFIG_DIR": Variable.get("GSPREAD_PANDAS_CONFIG_DIR")
            },
            *args,
            **kwargs,
        )


class GitCheckoutOperator(BashOperator):
    def __init__(self, dataset, branch, *args, **kwargs):
        bash_command = """\
        set -eu
        cd {{ params.dataset }}
        git checkout {{ params.branch }}
        """
        super().__init__(
            bash_command=bash_command,
            params={"dataset": dataset, "branch": branch},
            *args,
            **kwargs,
        )


class GitPullOperator(BashOperator):
    def __init__(self, dataset, *args, **kwargs):
        bash_command = """\
        set -eu
        cd {{ params.dataset }}
        git pull
        git submodule update --merge
        """
        super().__init__(
            bash_command=bash_command, params={"dataset": dataset}, *args, **kwargs
        )


class GitMergeOperator(BashOperator):
    def __init__(self, dataset, head, base, *args, **kwargs):
        bash_command = """\
        set -eu
        cd {{ params.dataset }}
        git checkout {{ params.base }}
        git merge {{ params.head }}
        """
        super().__init__(
            bash_command=bash_command,
            params={"dataset": dataset, "head": head, "base": base},
            *args,
            **kwargs,
        )


class GitPushOperator(BashOperator):
    """Check if there are updates, And push when necessary"""

    def __init__(self, dataset, push_all=False, *args, **kwargs):
        bash_command = """\
        set -eu
        cd {{ params.dataset }}
        {% if params.push_all is sameas true %}
        git checkout autogenerated
        git push -u origin
        git checkout develop
        git push -u origin
        git checkout master
        git push -u origin
        {% else %}
        git push -u origin
        {% endif %}
        """
        super().__init__(
            bash_command=bash_command,
            params={"dataset": dataset, "push_all": push_all},
            *args,
            **kwargs,
        )


class GitCommitOperator(BashOperator):
    """Check if there are updates, And make a commit when necessary.

    It will also push xcom when there is new commit.
    Excludes etl/.venv from being committed.
    """

    def __init__(self, dataset, *args, **kwargs):
        bash_command = """\
        set -eu
        cd {{ params.dataset }}
        # Exclude .venv from git status check
        if [[ $(git status -s -- ':!etl/.venv' | grep -e '^[? ][?D]' | head -c1 | wc -c) -ne 0 ]]; then
            git add . ':!etl/.venv'
            git commit -m "auto generated dataset"
            echo "git updated"
        else
            HAS_UPDATE=0
            for f in $(git diff --name-only -- ':!etl/.venv' ':!datapackage.json'); do
                if [[ $(git diff $f | tail -n +3 | grep -e "^[++|\-\-]" | head -c1 | wc -c) -ne 0 ]]; then
                    HAS_UPDATE=1
                    git add $f
                fi
            done
            if [[ $HAS_UPDATE -eq 1 ]]; then
                git add datapackage.json
                git commit -m "auto generated dataset"
                echo "git updated"
            else
                echo "nothing new"
            fi
        fi
        """
        super().__init__(
            bash_command=bash_command, params={"dataset": dataset}, *args, **kwargs
        )


class GitResetOperator(BashOperator):
    def __init__(self, dataset, *args, **kwargs):
        bash_command = """\
        set -eu
        cd {{ params.dataset }}
        export COMMIT=`git for-each-ref --format='%(upstream:short)' "$(git symbolic-ref -q HEAD)"`
        git reset --hard $COMMIT
        git clean -dfx
        """
        super().__init__(
            bash_command=bash_command, params={"dataset": dataset}, *args, **kwargs
        )


class GitResetAndGoMasterOperator(BashOperator):
    """reset current head and then checkout master branch"""

    def __init__(self, dataset, *args, **kwargs):
        bash_command = """\
        set -eu
        cd {{ params.dataset }}
        export BRANCH=`git rev-parse --abbrev-ref HEAD`
        case $BRANCH in
            master | develop)
                git reset --hard origin/$BRANCH
                git checkout autogenerated
                git reset --hard origin/autogenerated
                ;;
            autogenerated)
                git reset --hard origin/autogenerated
                ;;
        esac
        git clean -dfx
        git checkout master
        """
        super().__init__(
            bash_command=bash_command, params={"dataset": dataset}, *args, **kwargs
        )


class ValidateDatasetOperator(BashOperator):
    def __init__(self, dataset, logpath, *args, **kwargs):
        bash_command = """\
        export NODE_OPTIONS="--max-old-space-size=7000"
        cd {{ params.dataset }}
        validate-ddf-ng --no-warning ./
        if [ $? -eq 0 ]
        then
            sleep 2
            echo "validation succeed."
            exit 0
        else
            sleep 2
            echo "validation failed."
            exit 1
        fi
        """
        super().__init__(
            bash_command=bash_command,
            params={"dataset": dataset, "logpath": logpath},
            *args,
            **kwargs,
        )


class ValidateDatasetDependOnGitOperator(BashOperator):
    def __init__(self, dataset, logpath, *args, **kwargs):
        bash_command = """\
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
        """
        super().__init__(
            bash_command=bash_command,
            params={"dataset": dataset, "logpath": logpath},
            *args,
            **kwargs,
        )


class DependencyDatasetSensor(ExternalTaskSensor):
    """Sensor that waits for the most recent run of an external task to succeed.

    This sensor extends ExternalTaskSensor and uses XCom to find the most recent
    DAG run. The dependency DAG must have an emit_last_task_run_time task that
    pushes an XCom with key 'last_task_run_time' containing its logical_date.

    Args:
        external_dag_id: The DAG ID of the external DAG to wait for
        external_task_id: The task ID to check the status of (e.g., 'cleanup', 'validate')
        xcom_task_id: The task that pushes the XCom (default: 'emit_last_task_run_time')
        xcom_key: The XCom key to read (default: 'last_task_run_time')
        allowed_states: States considered as successful (default: ['success'])
        failed_states: States considered as failed (default: ['failed'])
        **kwargs: Additional arguments passed to ExternalTaskSensor
    """

    def __init__(
        self,
        external_dag_id: str,
        external_task_id: str,
        xcom_task_id: str = "emit_last_task_run_time",
        xcom_key: str = "last_task_run_time",
        allowed_states: list[str] | None = None,
        failed_states: list[str] | None = None,
        *args,
        **kwargs,
    ):
        self._external_dag_id = external_dag_id
        self._xcom_task_id = xcom_task_id
        self._xcom_key = xcom_key

        super().__init__(
            external_dag_id=external_dag_id,
            external_task_id=external_task_id,
            execution_date_fn=self._get_execution_date_from_xcom,
            allowed_states=allowed_states or ["success"],
            failed_states=failed_states or ["failed"],
            *args,
            **kwargs,
        )

    def _get_execution_date_from_xcom(self, logical_date, **context):
        """Get the execution date from XCom pushed by emit_last_task_run_time."""
        ti = context["ti"]
        last_run_time = ti.xcom_pull(
            dag_id=self._external_dag_id,
            task_ids=self._xcom_task_id,
            key=self._xcom_key,
        )

        if last_run_time:
            return [last_run_time]

        log.warning(
            f"No XCom '{self._xcom_key}' found for {self._external_dag_id}.{self._xcom_task_id}"
        )
        return []


class NotifyWaffleServerOperator(BashOperator):
    """Fake a slack command to load the dataset in waffle server"""

    def __init__(self, dataset, *args, **kwargs):
        base_name = dataset.split("/")[-1]
        conf = Variable.get("automatic_ws_datasets_conf", deserialize_json=True)
        if dataset in conf.keys():
            branch = conf[dataset]["branch"]
            ws_dataset_id = conf[dataset]["ws_dataset_id"]
        else:
            branch = "master"
            ws_dataset_id = "-".join(base_name.split("--")[1:]) + "-" + branch
        text = f"-N {ws_dataset_id} --publish https://github.com/{dataset}.git {branch}"

        bash_command = """\
        set -eu
        curl -d 'token=foo' -d 'command=/bwload' --data-urlencode 'text={{ params.text }}' http://35.228.158.102/slack/
        """

        super().__init__(
            bash_command=bash_command, params={"text": text}, *args, **kwargs
        )
