"""
A maintenance workflow that you can deploy into Airflow to periodically clean out /tmp (or other specified) folders to avoid those getting too big.
This DAG can only cleanup files created by the airflow user.
airflow trigger_dag --conf '{"maxLogAgeInDays":30}' server-tmp-cleanup
--conf options:
    maxTmpAgeInDays:<INT> - Optional
"""
from airflow.models import DAG, Variable
from airflow.configuration import conf
from airflow.operators.bash_operator import BashOperator
from datetime import timedelta
import os
import logging
import airflow


DAG_ID = os.path.basename(__file__).replace(".pyc", "").replace(".py", "")  # server-tmp-clean
START_DATE = airflow.utils.dates.days_ago(1)
SCHEDULE_INTERVAL = "@daily"        # How often to Run. @daily - Once a day at Midnight
DAG_OWNER_NAME = "airflow"          # Who is listed as the owner of this DAG in the Airflow Web Server
ALERT_EMAIL_ADDRESSES = []          # List of email address to send email alerts to if this job fails
DEFAULT_MAX_AGE_IN_DAYS = Variable.get("max_tmp_age_in_days", 1)  # Length to retain the log files if not already provided in the conf. If this is set to 30, the job will remove those files that are 30 days old or older
ENABLE_DELETE = True                # Whether the job should delete the logs or not. Included if you want to temporarily avoid deleting the logs
NUMBER_OF_WORKERS = 1               # The number of worker nodes you have in Airflow. Will attempt to run this process for however many workers there are so that each worker gets its logs cleared.
DIRECTORIES_TO_DELETE = ["/tmp"]
AIRFLOW_USER = "airflow"

default_args = {
    'owner': DAG_OWNER_NAME,
    'depends_on_past': False,
    'email': ALERT_EMAIL_ADDRESSES,
    'email_on_failure': True,
    'email_on_retry': False,
    'start_date': START_DATE,
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}

dag = DAG(DAG_ID, default_args=default_args, schedule_interval=SCHEDULE_INTERVAL, start_date=START_DATE)
if hasattr(dag, 'doc_md'):
    dag.doc_md = __doc__
if hasattr(dag, 'catchup'):
    dag.catchup = False


tmp_cleanup = """
echo "Getting Configurations..."
DIRECTORY="{{params.directory}}"
MAX_AGE_IN_DAYS="{{dag_run.conf.get('maxTmpAgeInDays', '')}}"
AIRFLOW_USER='""" + str(AIRFLOW_USER) + """'

if [ "${MAX_AGE_IN_DAYS}" == "" ]; then
    echo "maxTmpAgeInDays conf variable isn't included. Using Default '""" + str(DEFAULT_MAX_AGE_IN_DAYS) + """'."
    MAX_AGE_IN_DAYS='""" + str(DEFAULT_MAX_AGE_IN_DAYS) + """'
fi
ENABLE_DELETE=""" + str("true" if ENABLE_DELETE else "false") + """
echo "Finished Getting Configurations"
echo ""

echo "Configurations:"
echo "MAX_AGE_IN_DAYS:  '${MAX_AGE_IN_DAYS}'"
echo "ENABLE_DELETE:        '${ENABLE_DELETE}'"

echo ""
echo "Running Cleanup Process..."
FIND_STATEMENT="find ${DIRECTORY} -maxdepth 1 -mtime +${MAX_AGE_IN_DAYS} -user ${AIRFLOW_USER}"
DELETE_STMT="${FIND_STATEMENT} -exec rm -rf {} \;"

echo "Executing Find Statement: ${FIND_STATEMENT}"
FILES_MARKED_FOR_DELETE=`eval ${FIND_STATEMENT}`
echo "Process will be Deleting the following File(s)/Directory(s):"
echo "${FILES_MARKED_FOR_DELETE}"
echo "Process will be Deleting `echo "${FILES_MARKED_FOR_DELETE}" | grep -v '^$' | wc -l` File(s)/Directory(s)"     # "grep -v '^$'" - removes empty lines. "wc -l" - Counts the number of lines
echo ""
if [ "${ENABLE_DELETE}" == "true" ];
then
    if [ "${FILES_MARKED_FOR_DELETE}" != "" ];
    then
        echo "Executing Delete Statement: ${DELETE_STMT}"
        eval ${DELETE_STMT}
        DELETE_STMT_EXIT_CODE=$?
        if [ "${DELETE_STMT_EXIT_CODE}" != "0" ]; then
            echo "Delete process failed with exit code '${DELETE_STMT_EXIT_CODE}'"
            exit ${DELETE_STMT_EXIT_CODE}
        fi
    else
        echo "WARN: No File(s)/Directory(s) to Delete"
    fi
else
    echo "WARN: You're opted to skip deleting the File(s)/Directory(s)!!!"
fi
echo "Finished Running Cleanup Process"
"""
i = 0
for tmp_cleanup_id in range(1, NUMBER_OF_WORKERS + 1):

    for directory in DIRECTORIES_TO_DELETE:
        tmp_cleanup_dir_op = BashOperator(
            task_id='tmp_cleanup_directory_' + str(i),
            bash_command=tmp_cleanup,
            params={"directory": str(directory)},
            dag=dag)
        i += 1
