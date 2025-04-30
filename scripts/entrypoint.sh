#!/usr/bin/env bash

# create the admin user (idempotent: ignore errors if it already exists)
airflow users create \
  --username admin \
  --firstname Admin \
  --lastname User \
  --role Admin \
  --email admin@example.com \
  --password admin || true

# then launch the webserver
exec airflow webserver
