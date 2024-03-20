WITH data_1 AS (
    SELECT 
        ad_date,
        url_parameters,
        SUM(COALESCE(spend, 0)) AS spend,
        SUM(COALESCE(impressions, 0)) AS impressions,
        SUM(COALESCE(clicks, 0)) AS clicks,
        SUM(COALESCE(reach, 0)) AS reach,
        SUM(COALESCE(leads, 0)) AS leads,
        SUM(COALESCE(value, 0)) AS value
    FROM 
        facebook_ads_basic_daily fabd
    GROUP BY 
        ad_date, url_parameters
    UNION 
    SELECT
        ad_date,
        url_parameters,
        SUM(COALESCE(spend, 0)) AS spend,
        SUM(COALESCE(impressions, 0)) AS impressions,
        SUM(COALESCE(reach, 0)) AS reach,
        SUM(COALESCE(clicks, 0)) AS clicks,
        SUM(COALESCE(leads, 0)) AS leads,
        SUM(COALESCE(value, 0)) AS value
    FROM
        google_ads_basic_daily
    GROUP BY
        ad_date, url_parameters
),
data_2 AS (
    SELECT
        DATE_TRUNC('month', ad_date) AS ad_month,
        url_parameters,
        LOWER(
            CASE 
                WHEN SUBSTRING(url_parameters FROM 'utm_campaign=([^&#$%]+)') = 'nun' THEN NULL
                ELSE SUBSTRING(url_parameters FROM 'utm_campaign=([^&#$%]+)')
            END
        ) AS utm_campaign,
        SUM(spend) AS total_spend,
        SUM(impressions) AS total_impressions,
        SUM(clicks) AS total_clicks,
        SUM(value) AS total_value,
        CASE
            WHEN SUM(clicks) = 0 THEN 0
            ELSE SUM(value) / SUM(clicks)
        END AS CTR,
        CASE
            WHEN SUM(clicks) = 0 THEN 0
            ELSE SUM(spend) / SUM(clicks)
        END AS CPC,
        CASE
            WHEN SUM(impressions) = 0 THEN 0
            ELSE SUM(spend) / SUM(impressions) * 1000
        END AS CPM,
        CASE
            WHEN SUM(spend) = 0 THEN 0
            ELSE (SUM(value) - SUM(spend)) / SUM(spend)
        END AS ROMI
    FROM 
        data_1
    GROUP BY
        ad_month, url_parameters
),
data_3 AS (
    SELECT 
        ad_month,
        utm_campaign,
        total_spend,
        total_impressions,
        total_clicks,
        total_value,
        CPM,
        CTR,
        ROMI,
        ROUND(
            ((CPM - LAG(CPM) OVER (PARTITION BY utm_campaign ORDER BY ad_month)) / NULLIF(LAG(CPM) OVER (PARTITION BY utm_campaign ORDER BY ad_month), 0)) * 100,
            2
        ) AS diff_CPM_percentage,
        ROUND(
            ((CTR - LAG(CTR) OVER (PARTITION BY utm_campaign ORDER BY ad_month)) / NULLIF(LAG(CTR) OVER (PARTITION BY utm_campaign ORDER BY ad_month), 0)) * 100,
            2
        ) AS diff_CTR_percentage,
        ROUND(
            ((ROMI - LAG(ROMI) OVER (PARTITION BY utm_campaign ORDER BY ad_month)) / NULLIF(LAG(ROMI) OVER (PARTITION BY utm_campaign ORDER BY ad_month), 0)) * 100,
            2
        ) AS diff_ROMI_percentage
    FROM 
        data_2
)
SELECT 
    ad_month,
    utm_campaign,
    total_spend,
    total_impressions,
    total_clicks,
    total_value,
    CPM,
    CTR,
    ROMI,
    diff_CPM_percentage,
    diff_CTR_percentage,
    diff_ROMI_percentage
FROM 
    data_3;
