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
WITH SDF AS (
  SELECT
  CAST(SDFLineItem.Line_Item_Id AS INT64) AS lineitem_id,
  SDFLineItem.Type AS lineitem_type,
  SDFLineItem.End_Date AS lineitem_end_date,
  SDFLineItem.Pacing AS lineitem_pacing,
  SDFLineItem.Pacing_Rate AS lineitem_pacing_rate,
  SDFLineItem.Bid_Strategy_Value AS lineitem_bid_value,
  SDFInsertionOrder.Pacing AS io_pacing,
  SDFInsertionOrder.Pacing_Rate AS io_pacing_rate,
  SDFInsertionOrder.Performance_Goal_Type AS io_performance_goal_type,
  SDFCampaign.Campaign_Budget AS campaign_budget,
  SAFE_CAST(SDFLineItem.Frequency_Enabled AS BOOL) AS frequency_enabled_lineitem,
  SAFE_CAST(SDFInsertionOrder.Frequency_Enabled AS BOOL) AS frequency_enabled_io,
  IF(CAST(SDFCampaign.Frequency_Enabled AS BOOL) IS NULL, FALSE, CAST(SDFCampaign.Frequency_Enabled AS BOOL)) AS frequency_enabled_campaign, 
  IF(SDFLineItem.TrueView_View_Frequency_Enabled = "False" OR SDFLineItem.TrueView_View_Frequency_Enabled IS NULL, FALSE, TRUE) AS frequency_enabled_trueview,
  
-- Brand controls
  IF(SDFLineItem.Digital_Content_Labels_Exclude IS NULL, FALSE, TRUE) 
    AS bc_digital_content_label_exclusions,
  IF(SDFLineItem.Brand_Safety_Custom_Settings IS NULL
    AND SDFLineItem.TrueView_Category_Exclusions_Targeting IS NULL, FALSE, TRUE) 
    AS bc_sensitive_category_exclusions,
  IF(SDFLineItem.App_Collection_Targeting_Exclude IS NULL 
    AND SDFLineItem.App_Targeting_Exclude IS NULL 
    AND SDFLineItem.Site_Targeting_Exclude IS NULL
    AND SDFLineItem.Channel_Targeting_Exclude IS NULL
    AND SDFAdGroup.Placement_Targeting_YouTube_Channels_Exclude IS NULL
    AND SDFAdGroup.Placement_Targeting_YouTube_Videos_Exclude IS NULL
    AND SDFAdGroup.Placement_Targeting_URLs_Exclude IS NULL
    AND SDFAdGroup.Placement_Targeting_Apps_Exclude IS NULL
    AND SDFAdGroup.Placement_Targeting_App_Collections_Exclude IS NULL, FALSE, TRUE)
    AS bc_apps_url_channel_exclusions,
  IF(SDFLineItem.Keyword_Targeting_Exclude IS NULL 
    AND SDFLineItem.Keyword_List_Targeting_Exclude IS NULL
    AND SDFAdGroup.Keyword_Targeting_Exclude IS NULL, FALSE, TRUE) 
    AS bc_keyword_exclusions,
  IF(SDFLineItem.Inventory_Source_Targeting_Authorized_Seller_Options = 'Authorized Direct Sellers', TRUE, FALSE) AS bc_authorized_sellers_only,
  SDFLineItem.Inventory_Source_Targeting_Authorized_Seller_Options AS raw_lineitem_inventory_source_auth_seller,
  IF(SDFLineItem.Third_Party_Verification_Services = 'None'
    OR SDFLineItem.Third_Party_Verification_Services IS NULL, FALSE, TRUE) 
    AS bc_verification_services,
  
-- Targeting
  IF(SDFAdGroup.Placement_Targeting_YouTube_Channels_Exclude IS NULL
    AND SDFAdGroup.Placement_Targeting_YouTube_Videos_Exclude iS NULL, FALSE, TRUE)
    AS exclude_youtube_channel_video,
  IF(SDFLineItem.Bid_Multipliers IS NULL, FALSE, TRUE) AS bid_multipliers,
  IF(SDFLineItem.App_Collection_Targeting_Include IS NULL 
    AND SDFLineItem.App_Targeting_Include IS NULL 
    AND SDFLineItem.Site_Targeting_Include IS NULL 
    AND SDFLineItem.Channel_Targeting_Include IS NULL
    AND SDFAdGroup.Placement_Targeting_YouTube_Channels_Include IS NULL
    AND SDFAdGroup.Placement_Targeting_YouTube_Videos_Include IS NULL
    AND SDFAdGroup.Placement_Targeting_URLs_Include IS NULL
    AND SDFAdGroup.Placement_Targeting_Apps_Include IS NULL
    AND SDFAdGroup.Placement_Targeting_App_Collections_Include IS NULL, FALSE, TRUE) 
    AS include_apps_url_channel,
  SDFLineItem.App_Collection_Targeting_Include AS raw_lineitem_app_collection_include,
  SDFLineItem.App_Targeting_Include AS raw_lineitem_app_include,
  SDFLineItem.Site_Targeting_Include AS raw_lineitem_site_include,
  SDFLineItem.Channel_Targeting_Include AS raw_lineitem_channel_include,
  SDFAdGroup.Placement_Targeting_YouTube_Channels_Include AS raw_adgroup_placement_yt_channel_include,
  SDFAdGroup.Placement_Targeting_YouTube_Videos_Include AS raw_adgroup_placement_yt_video_include,
  SDFAdGroup.Placement_Targeting_URLs_Include AS raw_adgroup_placement_urls_include,
  SDFAdGroup.Placement_Targeting_Apps_Include AS raw_adgroup_placement_apps_include,
  SDFAdGroup.Placement_Targeting_App_Collections_Include AS raw_adgroup_placement_app_col_include,

  IF(SDFLineItem.Keyword_Targeting_Include IS NULL
    AND SDFAdGroup.Keyword_Targeting_Include IS NULL, FALSE, TRUE)
    AS include_keyword,
  SDFLineItem.Keyword_Targeting_Include AS raw_lineitem_keyword_include,
  SDFAdGroup.Keyword_Targeting_Include AS raw_adgroup_keyword_include,

  IF(SDFLineItem.Category_Targeting_Include IS NULL
    AND SDFLineItem.TrueView_Category_Exclusions_Targeting IS NULL
    AND SDFAdGroup.Category_Targeting_Include IS NULL, FALSE, TRUE)
    AS include_category,
  SDFLineItem.Category_Targeting_Include AS raw_lineitem_category_include,
  SDFLineItem.TrueView_Category_Exclusions_Targeting AS raw_lineitem_trueview_category_exclusions,
  SDFAdGroup.Category_Targeting_Include AS raw_adgroup_category_include,

  IF(SDFLineItem.Environment_Targeting IS NULL, FALSE, TRUE) AS include_environment,
  SDFLineItem.Environment_Targeting AS raw_lineitem_environment_include,

  IF(SDFLineItem.Language_Targeting_Include IS NULL, FALSE, TRUE) AS include_language,
  SDFLineItem.Language_Targeting_Include AS raw_lineitem_language_include,

  IF(SDFLineItem.Inventory_Source_Targeting_Include IS NULL, FALSE, TRUE) AS include_inventory,
  SDFLineItem.Inventory_Source_Targeting_Include AS raw_lineitem_inventory_include,

  IF(SDFLineItem.TrueView_Third_Party_Viewability_Vendor != 'None' 
    AND SDFLineItem.TrueView_Third_Party_Viewability_Vendor IS NOT NULL 
    OR SDFLineItem.Viewability_Targeting_Active_View IS NOT NULL, TRUE, FALSE)
    AS include_viewability,
  SDFLineItem.TrueView_Third_Party_Viewability_Vendor AS raw_lineitem_trueview_3p_viewability_include,
  SDFLineItem.Viewability_Targeting_Active_View AS raw_lineitem_viewability_active_view_include,

  IF(SDFLineItem.Audience_Targeting_Include IS NULL
    AND SDFAdGroup.Audience_Targeting_Include IS NULL, FALSE, TRUE)
    AS include_audience,
  SDFLineItem.Audience_Targeting_Include AS raw_lineitem_audience_include,
  SDFAdGroup.Audience_Targeting_Include AS raw_adgroup_audience_include,

  IF(SDFLineItem.Affinity_And_In_Market_Targeting_Include IS NULL
    AND SDFAdGroup.Affinity_And_In_Market_Targeting_Include IS NULL, FALSE, TRUE)
    AS include_affinity_inmarket,
  SDFLineItem.Affinity_And_In_Market_Targeting_Include AS raw_lineitem_affinity_inmarket_include,
  SDFAdGroup.Affinity_And_In_Market_Targeting_Include AS raw_adgroup_affinity_inmarket_include,

  IF(SDFLineItem.Geography_Targeting_Include IS NULL, FALSE, TRUE) AS include_geography,
  SDFLineItem.Geography_Targeting_Include AS raw_lineitem_geography_include,

  IF(SDFLineItem.Daypart_Targeting IS NULL, FALSE, TRUE) AS include_daypart,
  SDFLineItem.Daypart_Targeting AS raw_lineitem_daypart_include,

  IF(SDFLineItem.Demographic_Targeting_Gender IS NULL 
    AND SDFLineItem.Demographic_Targeting_Age IS NULL 
    AND SDFLineItem.Demographic_Targeting_Household_Income IS NULL 
    AND SDFLineItem.Demographic_Targeting_Parental_Status IS NULL
    AND SDFAdGroup.Demographic_Targeting_Gender IS NULL
    AND SDFAdGroup.Demographic_Targeting_Age IS NULL
    AND SDFAdGroup.Demographic_Targeting_Household_Income IS NULL
    AND SDFAdGroup.Demographic_Targeting_Parental_Status IS NULL, FALSE, TRUE) 
    AS include_demographic,

  SDFLineItem.Demographic_Targeting_Gender AS raw_lineitem_demo_gender,
  SDFLineItem.Demographic_Targeting_Age AS raw_lineitem_demo_age,
  SDFLineItem.Demographic_Targeting_Household_Income AS raw_lineitem_demo_income,
  SDFLineItem.Demographic_Targeting_Parental_Status AS raw_lineitem_demo_parental_status,
  SDFAdGroup.Demographic_Targeting_Gender AS raw_adgroup_demo_gender,
  SDFAdGroup.Demographic_Targeting_Age AS raw_adgroup_demo_age,
  SDFAdGroup.Demographic_Targeting_Household_Income AS raw_adgroup_demo_income,
  SDFAdGroup.Demographic_Targeting_Parental_Status AS raw_adgroup_demo_parental_status,

   IF(SDFLineItem.Position_Targeting_On_Screen IS NULL
    AND SDFLineItem.Position_Targeting_Display_Position_In_Content IS NULL
    AND SDFLineItem.Position_Targeting_Video_Position_In_Content IS NULL
    AND SDFLineItem.Position_Targeting_Audio_Position_In_Content IS NULL, FALSE, TRUE)
    AS include_position,
  SDFLineItem.Position_Targeting_On_Screen AS raw_lineitem_position_onscreen,
  SDFLineItem.Position_Targeting_Display_Position_In_Content AS raw_lineitem_display_position,
  SDFLineItem.Position_Targeting_Video_Position_In_Content AS raw_lineitem_video_position,
  SDFLineItem.Position_Targeting_Audio_Position_In_Content AS raw_lineitem_audio_position,

  IF(SDFLineItem.Browser_Targeting_Include IS NULL, FALSE, TRUE) AS include_browser,
  SDFLineItem.Browser_Targeting_Include AS raw_lineitem_browser_include,
  IF(SDFLineItem.Device_Targeting_Include IS NULL, FALSE, TRUE) AS include_device,
  SDFLineItem.Device_Targeting_Include AS raw_lineitem_device_include,
  IF(SDFLineItem.Connection_Speed_Targeting IS NULL, FALSE, TRUE) AS include_connection_speed,
  SDFLineItem.Connection_Speed_Targeting AS raw_lineitem_connection_speed_include,
  IF(SDFLineItem.Carrier_Targeting_Include IS NULL, FALSE, TRUE) AS include_carrier,
  SDFLineItem.Carrier_Targeting_Include AS raw_lineitem_carrier_include
  
  FROM `<CLOUD-PROJECT-ID>.<BQ-DATASET>.SDFLineItem` AS SDFLineItem
  LEFT JOIN `<CLOUD-PROJECT-ID>.<BQ-DATASET>.SDFAdGroup` AS SDFAdGroup USING (Line_Item_Id)
  LEFT JOIN `<CLOUD-PROJECT-ID>.<BQ-DATASET>.SDFInsertionOrder` AS SDFInsertionOrder USING (Io_Id)
  LEFT JOIN `<CLOUD-PROJECT-ID>.<BQ-DATASET>.SDFCampaign` AS SDFCampaign USING (Campaign_Id)
 ),

Report AS (
  SELECT
  SAFE_CAST(REPLACE(Report.Date,"/","-") AS DATE) AS report_date,
  SAFE_CAST(Report.Partner_ID AS INT64) AS partner_id,
  Report.Partner AS partner_name,
  SAFE_CAST(Report.Advertiser_ID AS INT64) AS advertiser_id,
  Report.Advertiser AS advertiser_name,
  SAFE_CAST(Report.Campaign_ID AS INT64) AS campaign_id,
  Report.Campaign AS campaign_name,
  SAFE_CAST(Report.Insertion_Order_ID AS INT64) AS io_id,
  Report.Insertion_Order AS io_name,
  Report.Line_Item AS lineitem_name,
  SAFE_CAST(Report.Line_Item_ID AS INT64) AS lineitem_id,
  -- Example naming convention check using regular expression. Checks LineItem name for given criteria
  REGEXP_CONTAINS(Report.Line_Item, r"\b(Display|Video|TrueView|display|video|trueview)\b") AS lineitem_naming_convention,
  SUM(SAFE_CAST(Report.Impressions AS INT64)) AS impressions,
  SUM(SAFE_CAST(Report.Clicks AS INT64)) AS clicks,
  ROUND(SUM(SAFE_CAST(Report.Revenue__USD_ AS FLOAT64)), 2) AS revenue_usd
  
  FROM `<CLOUD-PROJECT-ID>.<BQ-DATASET>.Reports` AS Report
  WHERE SAFE_CAST(REPLACE(Report.Date,"/","-") AS DATE) = DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
  AND Report.Date IS NOT NULL
  GROUP BY 1,2,3,4,5,6,7,8,9,10,11
),

Aggregations AS (
  SELECT *,
  
-- Flag definitions
  IF(frequency_enabled_lineitem = FALSE
    AND frequency_enabled_trueview = FALSE 
    AND frequency_enabled_io = FALSE
    AND frequency_enabled_campaign = FALSE, TRUE, FALSE)
    AS flag_frequency_cap,
  IF(bc_digital_content_label_exclusions = FALSE, TRUE, FALSE) AS flag_digital_content_label_exclusions,
  IF(bc_sensitive_category_exclusions = FALSE, TRUE, FALSE) AS flag_sensitive_category_exclusions,
  IF(bc_apps_url_channel_exclusions = FALSE, TRUE, FALSE) AS flag_apps_url_channel_exclusions,
  IF(lineitem_type = 'TrueView' AND exclude_youtube_channel_video = FALSE, TRUE, FALSE) AS flag_youtube_channel_video_exclusions
  
  FROM Report
  INNER JOIN SDF USING (lineitem_id)
)

SELECT 
  *,
  COUNTIF(flag_frequency_cap = TRUE)
    + COUNTIF(flag_digital_content_label_exclusions = TRUE)
    + COUNTIF(flag_sensitive_category_exclusions = TRUE)
    + COUNTIF(flag_apps_url_channel_exclusions = TRUE)
    + COUNTIF(flag_youtube_channel_video_exclusions = TRUE) AS total_flags
FROM Aggregations
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,
26,27,28,29,30,31,32,33,34,35,36,37,38,39,40,41,42,43,44,45,46,47,48,49,50,
51,52,53,54,55,56,57,58,59,60,61,62,63,64,65,66,67,68,69,70,71,72,73,74,75,
76,77,78,79,80,81,82,83,84,85,86,87,88,89,90,91,92,93,94,95,96,97,98,99,100
;