# gapminder-airflow-dags

An airflow DAG to automatically build/update open-numbers' repos.

## Setup in Airflow

Put/Symlink the three folder (dags, plugins, templates) in the repo
to your `$AIRFLOW_HOME` to use the DAGs. 

You need to define following Vairables in Airflow admin UI before the
DAGs can run:

- `airflow_home`: should be same as your `$AIRFLOW_HOME` environment variable
- `datasets_dir`: the path where the recipes/DAGs to find datasets

You also need an `etl` pool for etl related tasks. Set the
pool slot number to a reasonable value.

## Schedule

The DAG will run in a @daily schedule, and it will produce DAG for
each open-numbers' repo. Generated DAGs will run everyday at 0:10. 

If you need to change the schedule, you should modify not only the
`schedule_interval` in the update_all_datasets DAG, but also the
`schedule_interval` in the DAG templates. The `execute_date_fn` in
ExternalTaskSensor might need to change too.
