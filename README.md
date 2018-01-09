# gapminder-airflow-dags

An airflow DAG to automatically build/update open-numbers' repos.

Put/Symlink the three folder in the repo to your $AIRFLOW_HOME to use the DAG.
The DAG will run in a @daily schedule, and it will produce DAG for each open-numbers' repo.
Generated DAGs will run everyday at 0:10
