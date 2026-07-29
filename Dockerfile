# Extends the official Airflow image with dlt + Cosmos in the main
# environment, and dbt in an isolated virtualenv.
#
# `uv` already ships inside apache/airflow — this is the installer the
# official docs use for the extend-the-image path:
#   https://airflow.apache.org/docs/docker-stack/build.html
#
# Repeating "apache-airflow==${AIRFLOW_VERSION}" in the install command is not
# redundant: it stops a transitive dependency from silently upgrading or
# downgrading Airflow itself. AIRFLOW_VERSION is already set in the image.

FROM apache/airflow:3.3.0

# --- main environment: Airflow + dlt + Cosmos ---------------------------
COPY requirements.txt /tmp/requirements.txt

RUN uv pip install --no-cache \
      "apache-airflow==${AIRFLOW_VERSION}" \
      -r /tmp/requirements.txt

# --- isolated dbt environment -------------------------------------------
# /opt is root-owned, so the directory has to be created and handed to the
# airflow user before the unprivileged install step can write into it.
USER root
RUN mkdir -p /opt/dbt-venv && chown airflow:0 /opt/dbt-venv
USER airflow

COPY requirements-dbt.txt /tmp/requirements-dbt.txt

RUN uv venv /opt/dbt-venv \
    && uv pip install --no-cache --python /opt/dbt-venv/bin/python \
         -r /tmp/requirements-dbt.txt

# Cosmos invokes this path; it is also what you'd use for a manual
# `docker compose exec airflow-scheduler /opt/dbt-venv/bin/dbt debug`.
ENV DBT_EXECUTABLE_PATH=/opt/dbt-venv/bin/dbt

# For OS-level packages, switch to root and back:
#
# USER root
# RUN apt-get update \
#     && apt-get install -y --no-install-recommends some-package \
#     && apt-get clean && rm -rf /var/lib/apt/lists/*
# USER airflow
