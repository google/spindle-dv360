#!/bin/bash
###########################################################################
#
# Copyright 2020 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
###########################################################################

CONFIG_FILE="$1"

# check config file actually exists
if [ ! -f "$1" ]; then
    echo "'$1' does not exist, please check your file name and try again"
    return 1
fi

config() {
  local result=`cat "$CONFIG_FILE" | python3 -c "import sys, json; c = json.load(sys.stdin); d = '$2' if '$2' else ''; print(c.get('$1', d));"`
  echo "$result"
}

echo "----------------------------------------------------------------"
echo "Spindle setup in progress..."
echo "----------------------------------------------------------------"
# extract parameters from config file
echo ""
echo "Saving setup parameters from local config file:"
echo ""

# required parameters
ENVIRONMENT_NAME=$(config "environment_name")
echo "ENVIRONMENT_NAME="$ENVIRONMENT_NAME
CLOUD_PROJECT_ID=$(config "cloud_project_id")
echo "CLOUD_PROJECT_ID="$CLOUD_PROJECT_ID
BQ_DATASET=$(config "bq_dataset")
echo "BQ_DATASET="$BQ_DATASET
INSTALL_COMPOSER=$(config "install_composer")
echo "INSTALL_COMPOSER="$INSTALL_COMPOSER

# optional parameters
COMPOSER_BUCKET=$(config "composer_gcs_bucket")
echo "COMPOSER_BUCKET="$COMPOSER_BUCKET
COMPOSER_LOCATION=$(config "composer_location")
echo "COMPOSER_LOCATION="$COMPOSER_LOCATION
COMPOSER_ZONE=$(config "composer_zone")
echo "COMPOSER_ZONE="$COMPOSER_ZONE

# create new cloud composer instance when required
setup_cloud_composer() {
    echo ""
    echo "----------------------------------------------------------------"
    echo "Creating new Composer environment..."
    echo "----------------------------------------------------------------"
    echo ""
    gcloud composer environments create $ENVIRONMENT_NAME \
    --machine-type=n1-standard-1 \
    --python-version=3 \
    --disk-size=20GB \
    --oauth-scopes="https://www.googleapis.com/auth/cloud-platform,https://www.googleapis.com/auth/doubleclickbidmanager" \
    --location=$COMPOSER_LOCATION \
    --zone=$COMPOSER_ZONE

    # retrieve gcs bucket uri from new composer environment
    FULL_GCS_BUCKET_PATH=$(gcloud composer environments describe $ENVIRONMENT_NAME --location=$COMPOSER_LOCATION --format="value(config.dagGcsPrefix)")
    # validate that a new composer gcs bucket was successfully created
    if [ -z "$FULL_GCS_BUCKET_PATH" ]; then
        echo "Composer environment failed to generate, please re-run this script"
        return 1
    else
        # remove leading and trailing 5 characters
        COMPOSER_BUCKET=$(echo $FULL_GCS_BUCKET_PATH | sed 's/.....//;s/.....$//')
        echo "COMPOSER_BUCKET="$COMPOSER_BUCKET
    fi
    
    # create airflow variables
    echo "Creating Airflow variables..."
    gcloud composer environments run "${ENVIRONMENT_NAME}" --location="${COMPOSER_LOCATION}" variables --project="${CLOUD_PROJECT_ID}" -- --set "cloud_project_id" "${CLOUD_PROJECT_ID}"
    gcloud composer environments run "${ENVIRONMENT_NAME}" --location="${COMPOSER_LOCATION}" variables --project="${CLOUD_PROJECT_ID}" -- --set "gcs_bucket" "${COMPOSER_BUCKET}"
    gcloud composer environments run "${ENVIRONMENT_NAME}" --location="${COMPOSER_LOCATION}" variables --project="${CLOUD_PROJECT_ID}" -- --set "number_of_advertisers_per_sdf_api_call" "5"
    gcloud composer environments run "${ENVIRONMENT_NAME}" --location="${COMPOSER_LOCATION}" variables --project="${CLOUD_PROJECT_ID}" -- --set "sdf_api_version" "5"
    gcloud composer environments run "${ENVIRONMENT_NAME}" --location="${COMPOSER_LOCATION}" variables --project="${CLOUD_PROJECT_ID}" -- --set "sdf_bq_dataset" "${BQ_DATASET}"
    gcloud composer environments run "${ENVIRONMENT_NAME}" --location="${COMPOSER_LOCATION}" variables --project="${CLOUD_PROJECT_ID}" -- --set "sdf_file_types" "LINE_ITEM,AD_GROUP,AD,INSERTION_ORDER,CAMPAIGN"
    gcloud composer environments run "${ENVIRONMENT_NAME}" --location="${COMPOSER_LOCATION}" variables --project="${CLOUD_PROJECT_ID}" -- --set "dv360_sdf_advertisers" "{\"partner_id\": [\"advertiser_id1\", \"advertiser_id2\"]}"
    gcloud composer environments run "${ENVIRONMENT_NAME}" --location="${COMPOSER_LOCATION}" variables --project="${CLOUD_PROJECT_ID}" -- --set "partner_ids" "\"partner_id1\",\"partner_id2\""
}

# clone orchestra repo to local tmp directory
deploy_orchestra() {
    echo ""
    echo "----------------------------------------------------------------"
    echo "Deploying Orchestra to your Composer environment..."
    echo "----------------------------------------------------------------"
    echo ""
    # checks if orhcestra dir already exists
    if [ ! -d tmp/orchestra ]; then
        # clone orchestra v2.1.1 to avoid compatibility issues with future releases
        git clone --depth=1 --branch=2.1.1 "https://github.com/google/orchestra.git" "tmp/orchestra"
        echo "Cloned Orchestra repository to your /tmp directory"
    else
        echo "Orchestra repository already exists"
    fi

    # clean up and delete unused files from orchestra repo
    echo "Removing unused files.."
    find ./tmp/orchestra -mindepth 1 ! -regex '^./tmp/orchestra/orchestra/google/marketing_platform\(/.*\)?' -delete
    # delete 'example_dags' dir to minimise configuration issues in airflow
    find . -name "example_dags" -exec rm -rv {} +

    # sync required orchestra files to gcs composer bucket
    # warning: rsync -r will delete any files from the destination that do not exist in the source
    echo "Syncing Orchestra repo with GCS bucket..."
    gsutil -m rsync -r -d "./tmp/orchestra/orchestra/" "gs://$COMPOSER_BUCKET/dags/orchestra"
    
    # copy spindle dag to gcs composer bucket
    echo "Copying DAG to GCS bucket..."
    gsutil -m cp "./dags/spindle_dag.py" "gs://$COMPOSER_BUCKET/dags/"

    # find and replace placeholders in queries with actual project/dataset
    mkdir tmp/queries
    sed "s/<CLOUD-PROJECT-ID>.<BQ-DATASET>/${CLOUD_PROJECT_ID}.${BQ_DATASET}/g" queries/create_table.sql > ./tmp/queries/create_table.sql
    sed "s/<CLOUD-PROJECT-ID>.<BQ-DATASET>/${CLOUD_PROJECT_ID}.${BQ_DATASET}/g" queries/create_views.sql > ./tmp/queries/create_views.sql
    
    # copy spindle sql queries to gcs composer bucket
    echo "Copying SQL queries to GCS bucket..."
    gsutil cp -r "./tmp/queries" "gs://$COMPOSER_BUCKET/dags/"

    # copy report bigquery schema to gcs composer bucket
    echo "Copying BQ schema to GCS bucket..."
    gsutil cp -r "./schema" "gs://$COMPOSER_BUCKET/dags/"
}

main() {
    # check that values have been provided in .config for all required fields
    if [ -z "$ENVIRONMENT_NAME" ] || [ -z "$CLOUD_PROJECT_ID" ] || [ -z "$BQ_DATASET" ] || [ -z "$INSTALL_COMPOSER" ]; then 
        echo ""
        echo "----------------------------------------------------------------"
        echo "Deployment failed"
        echo "----------------------------------------------------------------"
        echo ""
        echo "'environment_name', 'cloud_project_id', 'bq_dataset' and 'install_composer' are all required fields"
        echo "Update your spindle.config file and re-run the script"
        echo ""
        return 1
    else
        # set the cloud project id
        gcloud config set project ${CLOUD_PROJECT_ID}
        # check user instructions to deploy new composer environment or deploy to existing
        if [ "$INSTALL_COMPOSER" = "true" ] || [ "$INSTALL_COMPOSER" = "True" ]; then
            setup_cloud_composer
            deploy_orchestra
            echo ""
            echo "----------------------------------------------------------------"
            echo "Cloud deployment completed"
            echo "----------------------------------------------------------------"
            echo ""
            echo "Intial deployment finished, please complete the remaining steps at github.com/google/spindle-dv360"
            echo ""
        else
            deploy_orchestra
            echo ""
            echo "----------------------------------------------------------------"
            echo "Cloud deployment completed"
            echo "----------------------------------------------------------------"
            echo ""
            echo "Intial deployment finished, please complete the remaining steps at github.com/google/spindle-dv360"
            echo ""
        fi
    fi
}

main "$@"
