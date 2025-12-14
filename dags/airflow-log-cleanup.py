"""
A maintenance workflow that you can deploy into Airflow to periodically clean
out the task logs to avoid those getting too big.

airflow dags trigger --conf '{"maxLogAgeInDays":30}' airflow-log-cleanup

--conf options:
    maxLogAgeInDays:<INT> - Optional
"""

from datetime import timedelta
import logging

import pendulum
from airflow.configuration import conf
from airflow.models import Variable
from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import dag


BASE_LOG_FOLDER = conf.get("logging", "BASE_LOG_FOLDER")
DAG_OWNER_NAME = "airflow"
ALERT_EMAIL_ADDRESSES = []
DEFAULT_MAX_LOG_AGE_IN_DAYS = Variable.get("max_log_age_in_days", 5)
ENABLE_DELETE = True
NUMBER_OF_WORKERS = 1
DIRECTORIES_TO_DELETE = [BASE_LOG_FOLDER]
ENABLE_DELETE_CHILD_LOG = Variable.get("enable_delete_child_log", "False")
logging.info("ENABLE_DELETE_CHILD_LOG  " + ENABLE_DELETE_CHILD_LOG)

if ENABLE_DELETE_CHILD_LOG.lower() == "true":
    try:
        CHILD_PROCESS_LOG_DIRECTORY = conf.get(
            "scheduler", "CHILD_PROCESS_LOG_DIRECTORY"
        )
        if CHILD_PROCESS_LOG_DIRECTORY != " ":
            DIRECTORIES_TO_DELETE.append(CHILD_PROCESS_LOG_DIRECTORY)
    except Exception as e:
        logging.exception(
            "Could not obtain CHILD_PROCESS_LOG_DIRECTORY from Airflow Configurations: "
            + str(e)
        )

LOG_CLEANUP_COMMAND = f"""
echo "Getting Configurations..."
BASE_LOG_FOLDER="{{{{params.directory}}}}"
TYPE="{{{{params.type}}}}"
MAX_LOG_AGE_IN_DAYS="{{{{dag_run.conf.get('maxLogAgeInDays', '')}}}}"
if [ "${{MAX_LOG_AGE_IN_DAYS}}" == "" ]; then
    echo "maxLogAgeInDays conf variable isn't included. Using Default '{DEFAULT_MAX_LOG_AGE_IN_DAYS}'."
    MAX_LOG_AGE_IN_DAYS='{DEFAULT_MAX_LOG_AGE_IN_DAYS}'
fi
ENABLE_DELETE={"true" if ENABLE_DELETE else "false"}
echo "Finished Getting Configurations"
echo ""

echo "Configurations:"
echo "BASE_LOG_FOLDER:      '${{BASE_LOG_FOLDER}}'"
echo "MAX_LOG_AGE_IN_DAYS:  '${{MAX_LOG_AGE_IN_DAYS}}'"
echo "ENABLE_DELETE:        '${{ENABLE_DELETE}}'"
echo "TYPE:                 '${{TYPE}}'"

echo ""
echo "Running Cleanup Process..."
if [ $TYPE == file ];
then
    FIND_STATEMENT="find ${{BASE_LOG_FOLDER}}/*/* -type f -mtime +${{MAX_LOG_AGE_IN_DAYS}}"
    DELETE_STMT="${{FIND_STATEMENT}} -exec rm -f {{}} \\;"
else
    FIND_STATEMENT="find ${{BASE_LOG_FOLDER}}/*/* -type d -empty"
    DELETE_STMT="${{FIND_STATEMENT}} -prune -exec rm -rf {{}} \\;"
fi
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
    dag_id="airflow-log-cleanup",
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
def airflow_log_cleanup():
    i = 0
    for log_cleanup_id in range(1, NUMBER_OF_WORKERS + 1):
        for directory in DIRECTORIES_TO_DELETE:
            log_cleanup_file_op = BashOperator(
                task_id=f"log_cleanup_file_{i}",
                bash_command=LOG_CLEANUP_COMMAND,
                params={"directory": str(directory), "type": "file"},
            )

            log_cleanup_dir_op = BashOperator(
                task_id=f"log_cleanup_directory_{i}",
                bash_command=LOG_CLEANUP_COMMAND,
                params={"directory": str(directory), "type": "directory"},
            )
            i += 1

            log_cleanup_file_op >> log_cleanup_dir_op


airflow_log_cleanup()
