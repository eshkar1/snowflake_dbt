
WITH parse_cursor_json AS (
    select * from {{ ref('stg_cursor_table')}}
)

SELECT
  period:startDate::NUMBER AS start_date_raw,
  period:endDate::NUMBER AS end_date_raw,
  TO_TIMESTAMP_NTZ(period:startDate::NUMBER / 1000) AS start_date,
  TO_TIMESTAMP_NTZ(period:endDate::NUMBER / 1000) AS end_date,
  TO_DATE(f.value:day::STRING, 'YYYY-MM-DD') AS day,
  f.value:email::STRING AS email,
  f.value:userId::STRING AS user_id,
  f.value:isActive::BOOLEAN AS is_active,
  f.value:clientVersion::STRING AS client_version,
  f.value:mostUsedModel::STRING AS most_used_model,
  f.value:applyMostUsedExtension::STRING AS apply_most_used_extension,
  f.value:tabMostUsedExtension::STRING AS tab_most_used_extension,
  f.value:acceptedLinesAdded::NUMBER AS accepted_lines_added,
  f.value:acceptedLinesDeleted::NUMBER AS accepted_lines_deleted,
  f.value:agentRequests::NUMBER AS agent_requests,
  f.value:apiKeyReqs::NUMBER AS api_key_reqs,
  f.value:bugbotUsages::NUMBER AS bugbot_usages,
  f.value:chatRequests::NUMBER AS chat_requests,
  f.value:cmdkUsages::NUMBER AS cmdk_usages,
  f.value:composerRequests::NUMBER  composer_requests,
  f.value:subscriptionIncludedReqs::NUMBER AS subscription_included_reqs,
  f.value:totalAccepts::NUMBER AS total_accepts,
  f.value:totalApplies::NUMBER AS total_applies,
  f.value:totalLinesAdded::NUMBER AS total_lines_added,
  f.value:totalLinesDeleted::NUMBER AS total_lines_deleted,
  f.value:totalRejects::NUMBER AS total_rejects,
  f.value:totalTabsAccepted::NUMBER AS total_tabs_accepted,
  f.value:totalTabsShown::NUMBER AS total_tabs_shown,
  f.value:usageBasedReqs::NUMBER AS usage_based_reqs
FROM parse_cursor_json,
LATERAL FLATTEN(input => data) f