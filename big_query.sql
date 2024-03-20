WITH sessions AS (
  SELECT
    DATE(TIMESTAMP_MICROS(event_timestamp)) AS event_date,
    CONCAT(user_pseudo_id, '_', MAX(IF(key = 'ga_session_id', value.int_value, NULL))) AS user_session_id,
    traffic_source.source AS source,
    traffic_source.medium AS medium,
    traffic_source.name AS campaign,
    MAX(IF(event_name = 'session_start', 1, 0)) AS session_start,
    MAX(IF(event_name = 'add_to_cart', 1, 0)) AS add_to_cart,
    MAX(IF(event_name = 'begin_checkout', 1, 0)) AS begin_checkout,
    MAX(IF(event_name = 'purchase', 1, 0)) AS purchase
  FROM
    `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_*`,
    UNNEST(event_params) AS param
  WHERE
    _TABLE_SUFFIX BETWEEN '20210101' AND '20211231'
    AND event_name IN ('session_start', 'add_to_cart', 'begin_checkout', 'purchase')
    AND param.key = 'ga_session_id'
  GROUP BY
    event_date,
    user_pseudo_id,
    source,
    medium,
    campaign
)

SELECT
  event_date,
  source,
  medium,
  campaign,
  COUNT(DISTINCT CASE WHEN session_start = 1 THEN user_session_id END) AS user_sessions_count,
  COUNT(DISTINCT CASE WHEN session_start = 1 AND add_to_cart = 1 THEN user_session_id END) AS visit_to_cart,
  COUNT(DISTINCT CASE WHEN session_start = 1 AND begin_checkout = 1 THEN user_session_id END) AS visit_to_checkout,
  COUNT(DISTINCT CASE WHEN session_start = 1 AND purchase = 1 THEN user_session_id END) AS visit_to_purchase
FROM
  sessions
GROUP BY
  event_date,
  source,
  medium,
  campaign
ORDER BY
  event_date
LIMIT 100;
