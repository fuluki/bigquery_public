WITH  hits 
            AS (SELECT user_pseudo_id, CONCAT(user_pseudo_id, 
              (SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id')) AS session_id, 
              event_date, event_timestamp, event_name, 
              IF(collected_traffic_source.manual_medium IN ('cpc', 'cpm'), collected_traffic_source.manual_source, NULL) AS 
              collected_source,
              IF(event_name = 'form_send', 1, 0) AS is_conversion  
              FROM `<dataset>`
              WHERE event_name NOT IN ('session_start', 'user_engagement', 'first_visit')
              ),

      window_hits 
            AS (SELECT *, ROW_NUMBER() OVER (PARTITION BY session_id ORDER BY event_timestamp) AS hit_number,
            SUM(is_conversion) OVER (PARTITION BY session_id) AS conversions_total                     
            FROM hits),

      sessions 
            AS (SELECT user_pseudo_id, session_id, event_date AS session_date, event_timestamp AS 
               session_start, collected_source, conversions_total FROM window_hits WHERE hit_number = 1),
      
      attributed_sessions
            AS (SELECT user_pseudo_id, session_id, session_date, session_start,
               collected_source, 

               IFNULL(collected_source, LAST_VALUE(collected_source IGNORE NULLS) OVER (PARTITION BY 
               user_pseudo_id ORDER BY session_start 
               RANGE BETWEEN 604800000000 PRECEDING AND 1 PRECEDING)) AS last_non_direct_source, #7days_attribution_window

               conversions_total FROM sessions)

SELECT IFNULL(last_non_direct_source, 'none') AS last_non_direct_source, SUM(conversions_total) AS conversions_total FROM attributed_sessions
GROUP BY 1
ORDER BY 2 DESC
