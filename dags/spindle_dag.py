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

"""Example DAG to create Spindle compatible pipeline
"""

from datetime import timedelta
from airflow import models
from airflow.utils import dates
from airflow.operators.sensors import TimeDeltaSensor
from airflow.operators.dummy_operator import DummyOperator
from airflow.contrib.operators.gcs_to_bq import GoogleCloudStorageToBigQueryOperator
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
import logging
import json

from orchestra.google.marketing_platform.operators.display_video_360 import (
  GoogleDisplayVideo360CreateReportOperator,
  GoogleDisplayVideo360RunReportOperator,
  GoogleDisplayVideo360DeleteReportOperator,
  GoogleDisplayVideo360RecordSDFAdvertiserOperator,
  GoogleDisplayVideo360SDFToBigQueryOperator,
  GoogleDisplayVideo360DownloadReportOperator
)
from orchestra.google.marketing_platform.sensors.display_video_360 import (
  GoogleDisplayVideo360ReportSensor
)

logger = logging.getLogger(__name__)

# --------------------------------------------------------------------------------
# Set variables
# --------------------------------------------------------------------------------

advertisers_report_definition = """{
  "params": {
    "type": "TYPE_GENERAL",
    "metrics": [
      "METRIC_IMPRESSIONS"
    ],
    "groupBys": [
      "FILTER_ADVERTISER",
      "FILTER_PARTNER"
    ],
    "filters": [
    {% for partner in params.partners %}
        {% if not loop.first %}, {% endif %}
        {"type": "FILTER_PARTNER", "value": {{ partner }}}
    {% endfor %}
    ]
  },
  "metadata": {
    "title": "spindle_v3_active_advertisers_report",
    "dataRange": "LAST_90_DAYS",
    "format": "csv"
  },
  "schedule": {
    "frequency": "ONE_TIME"
  }
}"""

perf_report_definition = """{
    "kind": "doubleclickbidmanager#query",
    "metadata": {
      "dataRange": "LAST_30_DAYS",
      "format": "CSV",
      "title": "spindle_v3_performance_report"
      },
    "schedule": {
      "frequency": "ONE_TIME"
      },
    "params": {
      "filters": [
      {% for partner in params.partners %}
        {% if not loop.first %}, {% endif %}
        {"type": "FILTER_PARTNER", "value": {{ partner }}}
      {% endfor %}],
      "groupBys": [
        "FILTER_DATE",
        "FILTER_PARTNER",
        "FILTER_ADVERTISER",
        "FILTER_ADVERTISER_CURRENCY",
        "FILTER_MEDIA_PLAN",
        "FILTER_INSERTION_ORDER",
        "FILTER_LINE_ITEM"
        ],
      "metrics": [
        "METRIC_IMPRESSIONS",
        "METRIC_BILLABLE_IMPRESSIONS",
        "METRIC_CLICKS",
        "METRIC_CTR",
        "METRIC_TOTAL_CONVERSIONS",
        "METRIC_LAST_CLICKS",
        "METRIC_LAST_IMPRESSIONS",
        "METRIC_REVENUE_ADVERTISER",
        "METRIC_MEDIA_COST_ADVERTISER",
        "METRIC_MEDIA_COST_USD",
        "METRIC_TOTAL_MEDIA_COST_USD",
        "METRIC_REVENUE_USD"
        ],
      "type": "TYPE_GENERAL"
      }
 }"""

cloud_project_id = models.Variable.get('cloud_project_id')
gcs_bucket = models.Variable.get("gcs_bucket")
partner_ids = models.Variable.get("partner_ids").split(",")
params = {"partners": partner_ids}
file_types = models.Variable.get('sdf_file_types').split(',')
api_version = models.Variable.get('sdf_api_version')
group_size = int(models.Variable.get('number_of_advertisers_per_sdf_api_call'))
advertisers_per_partner = models.Variable.get('dv360_sdf_advertisers')
advertisers_per_partner = json.loads(advertisers_per_partner)
sdf_bq_dataset = models.Variable.get('sdf_bq_dataset')

def group_advertisers(l, n):
  for i in range(0, len(l), n):
    yield l[i:i + n]

# --------------------------------------------------------------------------------
# Set default arguments
# --------------------------------------------------------------------------------

default_args = {
    "owner": "airflow",
    "start_date": dates.days_ago(1),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": timedelta(seconds=300),
}

# --------------------------------------------------------------------------------
# Main DAG
# --------------------------------------------------------------------------------

dag = models.DAG(
    'spindle_v3',
    default_args=default_args,
    catchup=False,
    default_view='graph',
    schedule_interval='0 22 * * *'  # pipeline will run every day at 10pm
)

create_report = GoogleDisplayVideo360CreateReportOperator(
    task_id="create_report",
    report=advertisers_report_definition,
    params={"partners": partner_ids},
    dag=dag
    )
query_id = "{{ task_instance.xcom_pull('create_report', key='query_id') }}"

run_report = GoogleDisplayVideo360RunReportOperator(
    task_id="run_report",
    query_id=query_id,
    dag=dag
    )

wait_for_report = GoogleDisplayVideo360ReportSensor(
    task_id="wait_for_report",
    query_id=query_id,
    dag=dag
    )
report_url = "{{ task_instance.xcom_pull('wait_for_report', key='report_url') }}"

record_advertisers = GoogleDisplayVideo360RecordSDFAdvertiserOperator(
    task_id='record_advertisers',
    report_url=report_url,
    variable_name='dv360_sdf_advertisers',
    dag=dag
    )

delete_report = GoogleDisplayVideo360DeleteReportOperator(
    task_id="delete_report",
    query_id=query_id,
    dag=dag
    )

create_performance_report = GoogleDisplayVideo360CreateReportOperator(
    task_id="create_performance_report",
    report=perf_report_definition,
    params={"partners": partner_ids},
    dag=dag
    )
query_id = "{{ task_instance.xcom_pull('create_performance_report', key='query_id') }}"

run_performance_report = GoogleDisplayVideo360RunReportOperator(
    task_id="run_performance_report",
    query_id=query_id,
    dag=dag
    )

wait_for_performance_report = GoogleDisplayVideo360ReportSensor(
    task_id="wait_for_performance_report",
    query_id=query_id,
    dag=dag
    )
performance_report_url = "{{ task_instance.xcom_pull('wait_for_performance_report', key='report_url') }}"

download_report_to_gcs = GoogleDisplayVideo360DownloadReportOperator(
    task_id="download_report_to_gcs",
    report_url=performance_report_url,
    destination_bucket=gcs_bucket,
    destination_object='reports/spindle_performance_report.csv',
    dag=dag
)

load_csv_to_bq = GoogleCloudStorageToBigQueryOperator(
    task_id='load_csv_to_bq',
    bucket=gcs_bucket,
    source_objects=['reports/spindle_performance_report.csv'],
    skip_leading_rows=1,
    max_bad_records=100,
    source_format='CSV',
    destination_project_dataset_table=f'{sdf_bq_dataset}.Reports',
    schema_object='dags/schema/report.json',
    write_disposition='WRITE_TRUNCATE',
    dag=dag
)

create_bq_table = BigQueryOperator(
    task_id='create_bq_table',
    use_legacy_sql=False,
    allow_large_results=True,
    sql='/queries/create_table.sql',
    create_disposition='CREATE_IF_NEEDED',
    write_disposition='WRITE_APPEND',
    time_partitioning={"type": "DAY", "expirationMs": "2592000000", "field": "report_date"},  # expires in 30 days
    destination_dataset_table=f'{cloud_project_id}.{sdf_bq_dataset}.spindle_lineitem_view',
    dag=dag
)

create_bq_views = BigQueryOperator(
    task_id='create_bq_views',
    use_legacy_sql=False,
    allow_large_results=True,
    sql='/queries/create_views.sql',
    dag=dag
)

start_sdf = DummyOperator(task_id='start_sdf', dag=dag)
end_sdf = DummyOperator(task_id='end_sdf', dag=dag, trigger_rule='all_done')

sdf_tasks = []
first = True
for partner, advertisers in advertisers_per_partner.items():
    advertiser_groups = group_advertisers(advertisers, group_size)
    for group_number, group in enumerate(advertiser_groups):
        message = 'RUNNING REQUESTS FOR A PARTNER: %s, ADVERTISERS: %s' % (
            partner, group)
        logger.info(message)
        task_id = 'upload_sdf_%s_%s_%s' % (partner, group_number, group[0])
        write_disposition = 'WRITE_APPEND'
        if first:
            write_disposition = 'WRITE_TRUNCATE'
        first = False
        task = GoogleDisplayVideo360SDFToBigQueryOperator(
            task_id=task_id,
            cloud_project_id=cloud_project_id,
            gcs_bucket=gcs_bucket,
            bq_dataset=sdf_bq_dataset,
            write_disposition=write_disposition,
            filter_ids=group,
            api_version=api_version,
            file_types=file_types,
            filter_type='ADVERTISER_ID',
            dag=dag)
        sdf_tasks.append(task)

create_report >> run_report >> wait_for_report >> record_advertisers >> delete_report >> start_sdf
start_sdf >> sdf_tasks[0:] >> end_sdf
end_sdf >> create_performance_report >> run_performance_report >> wait_for_performance_report >> download_report_to_gcs
download_report_to_gcs >> load_csv_to_bq >> create_bq_table >> create_bq_views
