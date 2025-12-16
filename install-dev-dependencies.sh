#! /usr/bin/env bash
#
AIRFLOW_VERSION=3.1.3

### For production airflow setup.
# Extract the version of Python you have installed. If you're currently using a Python version that is not supported by Airflow, you may want to set this manually.
# See above for supported versions.
# PYTHON_VERSION="$(python -c 'import sys; print(f"{sys.version_info.major}.{sys.version_info.minor}")')"

# CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"
# For example this would install 3.0.0 with python 3.10: https://raw.githubusercontent.com/apache/airflow/constraints-3.1.5/constraints-3.10.txt

# uv pip install "apache-airflow==${AIRFLOW_VERSION}" --constraint "${CONSTRAINT_URL}"

# But when developing the plugins, we don't need to have to get "golden version" for all packages. So let uv decide the constraints.
uv pip install ddf_utils==1.0.18 "apache-airflow==${AIRFLOW_VERSION}" apache-airflow-providers-http apache-airflow-client
