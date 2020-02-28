/*
 * Copyright 2020 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * 
 */

-- standardSQL
-- ----------------------------------------------------------------------------
-- Partner view
-- ----------------------------------------------------------------------------
CREATE OR REPLACE VIEW `<CLOUD-PROJECT-ID>.<BQ-DATASET>.spindle_partner_view`
OPTIONS(
 expiration_timestamp=TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL 30 DAY),
 friendly_name="Spindle partner-level view",
 description="Spindle Partner view, expires in 30 days"
)
AS 
  SELECT
  partner_id,
  partner_name,

-- Brand controls
  ROUND(COUNTIF(bc_digital_content_label_exclusions = TRUE) / COUNT(bc_digital_content_label_exclusions),2) AS bc_digital_content_label_exclusions_pct,
  ROUND(COUNTIF(bc_sensitive_category_exclusions = TRUE) / COUNT(bc_sensitive_category_exclusions),2) AS bc_sensitive_category_exclusions_pct,
  ROUND(COUNTIF(bc_apps_url_channel_exclusions = TRUE) / COUNT(bc_apps_url_channel_exclusions),2) AS bc_apps_url_channel_exclusions_pct,
  ROUND(COUNTIF(bc_keyword_exclusions = TRUE) / COUNT(bc_keyword_exclusions),2) AS bc_keyword_exclusions_pct,
  ROUND(COUNTIF(bc_authorized_sellers_only = TRUE) / COUNT(bc_authorized_sellers_only),2) AS bc_authorized_sellers_only_pct,
  ROUND(COUNTIF(bc_verification_services = TRUE) / COUNT(bc_verification_services),2) AS bc_verification_services_pct,

-- Best practice
  ROUND(COUNTIF(io_performance_goal_type != 'None') / COUNT(io_performance_goal_type),2) AS automated_bidding_adoption_pct,
  ROUND(COUNTIF(include_audience = TRUE) / COUNT(include_audience),2) AS include_audience_pct,
  ROUND(COUNTIF(include_geography = TRUE) / COUNT(include_geography),2) AS include_geography_pct,
  ROUND(COUNTIF(include_category = TRUE) / COUNT(include_category),2) AS include_category_pct,
  ROUND(COUNTIF(include_viewability = TRUE) / COUNT(include_viewability),2) AS include_viewability_pct,
  ROUND(COUNTIF(include_demographic = TRUE) / COUNT(include_demographic),2) AS include_demographic_pct,
  ROUND(COUNTIF(include_affinity_inmarket = TRUE) / COUNT(include_affinity_inmarket),2) AS include_affinity_inmarket_pct,
  ROUND(COUNTIF(frequency_enabled_campaign = TRUE OR frequency_enabled_io = TRUE OR frequency_enabled_lineitem = TRUE) / COUNT(frequency_enabled_lineitem),2) AS frequency_capping_pct,
  ROUND(COUNTIF(include_apps_url_channel = TRUE) / COUNT(include_apps_url_channel),2) AS include_apps_url_channel_pct

FROM `<CLOUD-PROJECT-ID>.<BQ-DATASET>.spindle_lineitem_view`
GROUP BY 1,2
;

-- ----------------------------------------------------------------------------
-- Advertiser view
-- ----------------------------------------------------------------------------
CREATE OR REPLACE VIEW `<CLOUD-PROJECT-ID>.<BQ-DATASET>.spindle_advertiser_view`
OPTIONS(
 expiration_timestamp=TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL 30 DAY),
 friendly_name="Spindle advertiser-level view",
 description="Spindle Advertiser view, expires in 30 days"
)
AS 
  SELECT
  partner_id,
  partner_name,
  advertiser_id,
  advertiser_name,

-- Brand controls
  ROUND(COUNTIF(bc_digital_content_label_exclusions = TRUE) / COUNT(bc_digital_content_label_exclusions),2) AS bc_digital_content_label_exclusions_pct,
  ROUND(COUNTIF(bc_sensitive_category_exclusions = TRUE) / COUNT(bc_sensitive_category_exclusions),2) AS bc_sensitive_category_exclusions_pct,
  ROUND(COUNTIF(bc_apps_url_channel_exclusions = TRUE) / COUNT(bc_apps_url_channel_exclusions),2) AS bc_apps_url_channel_exclusions_pct,
  ROUND(COUNTIF(bc_keyword_exclusions = TRUE) / COUNT(bc_keyword_exclusions),2) AS bc_keyword_exclusions_pct,
  ROUND(COUNTIF(bc_authorized_sellers_only = TRUE) / COUNT(bc_authorized_sellers_only),2) AS bc_authorized_sellers_only_pct,
  ROUND(COUNTIF(bc_verification_services = TRUE) / COUNT(bc_verification_services),2) AS bc_verification_services_pct,

-- Best practice
  ROUND(COUNTIF(io_performance_goal_type != 'None') / COUNT(io_performance_goal_type),2) AS automated_bidding_adoption_pct,
  ROUND(COUNTIF(include_audience = TRUE) / COUNT(include_audience),2) AS include_audience_pct,
  ROUND(COUNTIF(include_geography = TRUE) / COUNT(include_geography),2) AS include_geography_pct,
  ROUND(COUNTIF(include_category = TRUE) / COUNT(include_category),2) AS include_category_pct,
  ROUND(COUNTIF(include_viewability = TRUE) / COUNT(include_viewability),2) AS include_viewability_pct,
  ROUND(COUNTIF(include_demographic = TRUE) / COUNT(include_demographic),2) AS include_demographic_pct,
  ROUND(COUNTIF(include_affinity_inmarket = TRUE) / COUNT(include_affinity_inmarket),2) AS include_affinity_inmarket_pct,
  ROUND(COUNTIF(frequency_enabled_campaign = TRUE OR frequency_enabled_io = TRUE OR frequency_enabled_lineitem = TRUE) / COUNT(frequency_enabled_lineitem),2) AS frequency_capping_pct,
  ROUND(COUNTIF(include_apps_url_channel = TRUE) / COUNT(include_apps_url_channel),2) AS include_apps_url_channel_pct

FROM `<CLOUD-PROJECT-ID>.<BQ-DATASET>.spindle_lineitem_view`
GROUP BY 1,2,3,4
;

-- ----------------------------------------------------------------------------
-- Campaign view
-- ----------------------------------------------------------------------------
CREATE OR REPLACE VIEW `<CLOUD-PROJECT-ID>.<BQ-DATASET>.spindle_campaign_view`
OPTIONS(
 expiration_timestamp=TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL 30 DAY),
 friendly_name="Spindle campaign-level view",
 description="Spindle Campaign view, expires in 30 days"
)
AS
  SELECT
  partner_id,
  partner_name,
  advertiser_id,
  advertiser_name,
  campaign_id,
  campaign_name,

-- Brand controls
  ROUND(COUNTIF(bc_digital_content_label_exclusions = TRUE) / COUNT(bc_digital_content_label_exclusions),2) AS bc_digital_content_label_exclusions_pct,
  ROUND(COUNTIF(bc_sensitive_category_exclusions = TRUE) / COUNT(bc_sensitive_category_exclusions),2) AS bc_sensitive_category_exclusions_pct,
  ROUND(COUNTIF(bc_apps_url_channel_exclusions = TRUE) / COUNT(bc_apps_url_channel_exclusions),2) AS bc_apps_url_channel_exclusions_pct,
  ROUND(COUNTIF(bc_keyword_exclusions = TRUE) / COUNT(bc_keyword_exclusions),2) AS bc_keyword_exclusions_pct,
  ROUND(COUNTIF(bc_authorized_sellers_only = TRUE) / COUNT(bc_authorized_sellers_only),2) AS bc_authorized_sellers_only_pct,
  ROUND(COUNTIF(bc_verification_services = TRUE) / COUNT(bc_verification_services),2) AS bc_verification_services_pct,

-- Best practice
  ROUND(COUNTIF(io_performance_goal_type != 'None') / COUNT(io_performance_goal_type),2) AS automated_bidding_adoption_pct,
  ROUND(COUNTIF(include_audience = TRUE) / COUNT(include_audience),2) AS include_audience_pct,
  ROUND(COUNTIF(include_geography = TRUE) / COUNT(include_geography),2) AS include_geography_pct,
  ROUND(COUNTIF(include_category = TRUE) / COUNT(include_category),2) AS include_category_pct,
  ROUND(COUNTIF(include_viewability = TRUE) / COUNT(include_viewability),2) AS include_viewability_pct,
  ROUND(COUNTIF(include_demographic = TRUE) / COUNT(include_demographic),2) AS include_demographic_pct,
  ROUND(COUNTIF(include_affinity_inmarket = TRUE) / COUNT(include_affinity_inmarket),2) AS include_affinity_inmarket_pct,
  ROUND(COUNTIF(frequency_enabled_campaign = TRUE OR frequency_enabled_io = TRUE OR frequency_enabled_lineitem = TRUE) / COUNT(frequency_enabled_lineitem),2) AS frequency_capping_pct,
  ROUND(COUNTIF(include_apps_url_channel = TRUE) / COUNT(include_apps_url_channel),2) AS include_apps_url_channel_pct

FROM `<CLOUD-PROJECT-ID>.<BQ-DATASET>.spindle_lineitem_view`
GROUP BY 1,2,3,4,5,6
;