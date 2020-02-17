# Spindle

## What is Spindle?

Spindle is a QA (quality assurance) dashboard that can be used to identify financial or reputational risk across multiple Display & Video 360 (DV360) advertisers, surfaced in a fully configurable Data Studio dashboard. It uses a set of predefined ‘flags’ to stack rank DV360 entities according to potential risk. Flags can be reconfigured as required to check for personalized best practices (e.g. naming conventions, brand safety, etc).

Spindle leverages [Orchestra](https://github.com/google/orchestra), an open source data pipeline and orchestration tool developed on Google Cloud Composer. This repository is intended to streamline the deployment of Spindle, and required components from Orchestra, to a new or existing Cloud Composer environment.

**This is not an officially supported Google product.**

### Architecture

![alt text](docs/images/spindle_architecture.png "Spindle Architecture diagram")

## Installation

The easiest and quickest way to deploy Spindle, is by using this deployment script. New deployments should take ~25 minutes to complete. Deployments to existing Composer environments should finish in under 5 mins.

### Before you begin

#### Prepare your Cloud project
This script will use the default service credentials for Compute Engine ("Compute Engine default service account"). These service accounts have access to all Cloud APIs enabled for your Cloud project, making them a good fit for Spindle/Orchestra.

*   Make sure that billing is enabled for your Google Cloud project.
*   Enable the [Cloud Composer](https://console.cloud.google.com/apis/library/composer.googleapis.com) and [DoubleClick Bid Manager API](https://console.cloud.google.com/apis/api/doubleclickbidmanager.googleapis.com/overview) from the [API Library](https://console.cloud.google.com/apis/library)
*   **Important**: create your destination BigQuery dataset **before** deploying your pipeline. The Spindle DAG will fail if the dataset does not exist prior to execution. This is to prevent accidentally overwriting an existing dataset.

> :exclamation: If deploying to an existing Composer environment, ensure it has DoubleClick Bid Manager included in the 'Google API scopes' or Airflow connection.

#### Create Display & Video 360 user

Your Compute Engine service account email (similar to XX-compute@developer.gserviceaccount.com) will need to be setup as a DV360 user so that it can access the required data from your DV360 account. DV360 partner-level access is required to [create a new user](https://support.google.com/displayvideo/answer/2723011?hl=en). New DV360 users should be created using this configuration:

*   'User email' should be the email associated with your default service account
*   Select all Partner IDs you wish to access with Spindle
*   Ensure the user is given 'Read & write' access permissions

### Pipeline setup

1.   Activate the [Cloud Shell](https://cloud.google.com/shell/docs/using-cloud-shell) and then 'Launch Editor'
2.   From the command Shell, clone this repository

```shell
git clone https://github.com/google/spindle
```
3.   From the editor window open the configuration file  `spindle.config`

For deployments to a ***new*** Composer instance, follow this template:

| Field |  | Description | Example |
| :----- | :--------- | :----- | :---------- |
| cloud_project_id| Required | Google Cloud Project ID | my-gcp-project1  |
| bq_dataset | Required | BigQuery destination dataset name | spindle_client |
| install_composer| Required | Indicates whether to create a new Composer instance | true  |
| composer_location| Required | Compute Engine [region](https://cloud.google.com/compute/docs/regions-zones#locations) | europe-west2  |
| composer_zone | Required | Compute Engine [zone](https://cloud.google.com/compute/docs/regions-zones#locations) | europe-west1-c |
| composer_gcs_bucket | Optional | Cloud Storage bucket URI. Do not include leading 'gs://' or trailing '/dags' | Leave blank for new Composer instances |

For deployments to an ***existing*** Composer instance:

| Field |  | Description | Example |
| :----- | :--------- | :----- | :---------- |
| cloud_project_id| Required | Google Cloud Project ID | my-gcp-project1  |
| bq_dataset | Required | BigQuery destination dataset name  | spindle_client |
| install_composer| Required | Indicates whether to create a new Composer instance | false  |
| composer_gcs_bucket | Required | Cloud Storage bucket URI. Do not include leading 'gs://' or trailing '/dags' | europe-west2-spindle-42cn4da-bucket |

4.  After updating `spindle.config`, execute the following in the Shell:

```shell
cd spindle
. deploy.sh spindle.config
```

If you used the script to create a new Composer instance, most of the configuration has been done for you, but you'll still need to update your partner IDs.

5.  Open your 'Airflow webserver' from the  [Composer environments list](https://console.cloud.google.com/composer/environments). Navigate to Admin >> Variables, update the variable 'partner_ids'. Partner IDs must be surrounded by double quotes and comma-separated e.g. "1234", "5678".

If you're using an existing Composer environment, variables must be created manually, as detailed [here](https://github.com/google/orchestra#variables). This is to prevent overwriting an existing configuration.

### Dashboard setup

By default, the Spindle DAG is configured to run at 10PM daily. This means that your BigQuery datasets will not be generated until the first run has finished. Once your pipeline has finished for the first time, you can proceed with dashboard setup.

> :warning: **Warning:** manually triggering the DAG before the schedule (10pm) may result in duplicate rows in your dataset.

1.   Make copies of the x4 Data Studio data source templates:
*   Open [LineItem view](#) -- click 'Make a copy of this datasource`, then 'Edit connection', and associate the new data source with the corresponding table in your BigQuery dataset: 'spindle_lineitem_view'.
*   Open [Partner view](#) -- click 'Make a copy of this datasource', then 'Edit connection', and associate the new data source with the corresponding view in your BigQuery dataset: 'spindle_partner_view'.
*   Open [Advertiser view](#) -- click 'Make a copy of this datasource', then 'Edit connection', and associate the new data source with the corresponding view in your BigQuery dataset: 'spindle_advertiser_view'.
*   Open [Campaign view](#) -- click 'Make a copy of this datasource', then 'Edit connection', and associate the new data source with the corresponding view in your BigQuery dataset: 'spindle_campaign_view'.
2.   Open the master Data Studio [report template](#) -- after copying the report, you'll be prompted to select new datasources. Swap out the 'Original data source' with the copies you created in the previous steps and click 'Copy Report'.
3.   Your report should now be ready to share with other users.

> :warning: **Warning:** sharing the Data Studio dashboard with other users, will allow them to view all data in dashboard, regardless of whether they have the underlying permissions to view the DV360 Partners/Advertisers featured in the dashboard.

## Data & Privacy

[Orchestra](https://github.com/google/orchestra) is a framework that allows powerful API access to your data. Liability for how you use that data is your own. It is important that all data you keep is secure and that you have legal permission to work and transfer all data you use. Orchestra can operate across multiple Partners, please be sure that this access is covered by legal agreements with your clients before implementing Orchestra. This project is covered by the Apache License.