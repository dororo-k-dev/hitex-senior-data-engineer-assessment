#!/bin/bash

echo "Deploying HITEx Data Platform..."

# Validate DAGs
python -c "
from airflow.models import DagBag
dag_bag = DagBag()
if dag_bag.import_errors:
    print('DAG validation errors:')
    for dag_id, error in dag_bag.import_errors.items():
        print(f'{dag_id}: {error}')
    exit(1)
print('All DAGs validated successfully')
"

# Deploy to Cloud Composer (if environment variables set)
if [ ! -z "$COMPOSER_ENVIRONMENT" ] && [ ! -z "$GCP_PROJECT" ]; then
    gcloud composer environments storage dags import \
        --environment $COMPOSER_ENVIRONMENT \
        --location us-central1 \
        --source dags/
    echo "DAGs deployed to Cloud Composer"
else
    echo "Skipping Cloud Composer deployment - environment variables not set"
fi

echo "Deployment completed successfully"