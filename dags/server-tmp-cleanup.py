"""
A maintenance workflow that you can deploy into Airflow to periodically clean
out /tmp (or other specified) folders to avoid those getting too big.
This DAG can only cleanup files created by the airflow user.

airflow dags trigger --conf '{"maxTmpAgeInDays":30}' server-tmp-cleanup

--conf options:
    maxTmpAgeInDays:<INT> - Optional
"""

from datetime import timedelta

import pendulum
from airflow.models import Variable
from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import dag


DAG_OWNER_NAME = "airflow"
ALERT_EMAIL_ADDRESSES = []
DEFAULT_MAX_AGE_IN_DAYS = Variable.get("max_tmp_age_in_days", 1)
ENABLE_DELETE = True
NUMBER_OF_WORKERS = 1
DIRECTORIES_TO_DELETE = ["/tmp"]
AIRFLOW_USER = "airflow"

TMP_CLEANUP_COMMAND = f"""
echo "Getting Configurations..."
DIRECTORY="{{{{params.directory}}}}"
MAX_AGE_IN_DAYS="{{{{dag_run.conf.get('maxTmpAgeInDays', '')}}}}"
AIRFLOW_USER='{AIRFLOW_USER}'

if [ "${{MAX_AGE_IN_DAYS}}" == "" ]; then
    echo "maxTmpAgeInDays conf variable isn't included. Using Default '{DEFAULT_MAX_AGE_IN_DAYS}'."
    MAX_AGE_IN_DAYS='{DEFAULT_MAX_AGE_IN_DAYS}'
fi
ENABLE_DELETE={"true" if ENABLE_DELETE else "false"}
echo "Finished Getting Configurations"
echo ""

echo "Configurations:"
echo "MAX_AGE_IN_DAYS:  '${{MAX_AGE_IN_DAYS}}'"
echo "ENABLE_DELETE:        '${{ENABLE_DELETE}}'"

echo ""
echo "Running Cleanup Process..."
FIND_STATEMENT="find ${{DIRECTORY}} -maxdepth 1 -mtime +${{MAX_AGE_IN_DAYS}} -user ${{AIRFLOW_USER}}"
DELETE_STMT="${{FIND_STATEMENT}} -exec rm -rf {{}} \\;"

echo "Executing Find Statement: ${{FIND_STATEMENT}}"
FILES_MARKED_FOR_DELETE=`eval ${{FIND_STATEMENT}}`
echo "Process will be Deleting the following File(s)/Directory(s):"
echo "${{FILES_MARKED_FOR_DELETE}}"
echo "Process will be Deleting `echo "${{FILES_MARKED_FOR_DELETE}}" | grep -v '^$' | wc -l` File(s)/Directory(s)"
echo ""
if [ "${{ENABLE_DELETE}}" == "true" ];
then
    if [ "${{FILES_MARKED_FOR_DELETE}}" != "" ];
    then
        echo "Executing Delete Statement: ${{DELETE_STMT}}"
        eval ${{DELETE_STMT}}
        DELETE_STMT_EXIT_CODE=$?
        if [ "${{DELETE_STMT_EXIT_CODE}}" != "0" ]; then
            echo "Delete process failed with exit code '${{DELETE_STMT_EXIT_CODE}}'"
            exit ${{DELETE_STMT_EXIT_CODE}}
        fi
    else
        echo "WARN: No File(s)/Directory(s) to Delete"
    fi
else
    echo "WARN: You're opted to skip deleting the File(s)/Directory(s)!!!"
fi
echo "Finished Running Cleanup Process"
"""


@dag(
    dag_id="server-tmp-cleanup",
    schedule="@daily",
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    catchup=False,
    doc_md=__doc__,
    default_args={
        "owner": DAG_OWNER_NAME,
        "depends_on_past": False,
        "email": ALERT_EMAIL_ADDRESSES,
        "email_on_failure": True,
        "email_on_retry": False,
        "retries": 1,
        "retry_delay": timedelta(minutes=1),
    },
)
def server_tmp_cleanup():
    i = 0
    for _ in range(1, NUMBER_OF_WORKERS + 1):
        for directory in DIRECTORIES_TO_DELETE:
            BashOperator(
                task_id=f"tmp_cleanup_directory_{i}",
                bash_command=TMP_CLEANUP_COMMAND,
                params={"directory": str(directory)},
            )
            i += 1


server_tmp_cleanup()
