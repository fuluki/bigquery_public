WITH  hits 
            AS (SELECT user_pseudo_id, CONCAT(user_pseudo_id, 
               (SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id')) 
               AS session_id, 
               event_date, event_timestamp, event_name, collected_traffic_source.manual_source 
               AS 
               collected_source,
               IF(event_name = 'form_send', 1, 0) AS is_conversion  
               FROM `******.analytics_*********.events_2023*`
               WHERE event_name NOT IN ('session_start', 'user_engagement', 'first_visit')

              AND user_pseudo_id IN (SELECT DISTINCT user_pseudo_id 
              FROM `gtm-k4v7jns-mjziz.analytics_334843133.events_2023*` WHERE event_name = 
              'form_send')),

      window_hits 
            AS (SELECT *, ROW_NUMBER() OVER (PARTITION BY session_id ORDER BY event_timestamp) 
            AS hit_number,
            SUM(is_conversion) OVER (PARTITION BY session_id) 
            AS conversions_total                     
            FROM hits),

      sessions 
            AS (SELECT user_pseudo_id, event_date AS session_date, event_timestamp AS 
            session_start, collected_source, 
    
            conversions_total,

            IF(conversions_total > 0, event_timestamp, 0) AS conversion_time 

            FROM window_hits WHERE hit_number = 1),

      collected_sessions 
            AS (SELECT *, 
            MAX(conversion_time) OVER (PARTITION BY user_pseudo_id) - session_start AS 
            window_diff FROM sessions),
      
      sequences 
            AS (SELECT user_pseudo_id, session_date, session_start, collected_source, 
            conversions_total,

            CASE 
      
            WHEN MAX(collected_source) OVER (PARTITION BY user_pseudo_id) IS NULL THEN SUM 
            (conversions_total) OVER (PARTITION 
            BY user_pseudo_id) / COUNT(session_date) OVER (PARTITION BY user_pseudo_id)

            WHEN collected_source IS NULL THEN 0

            ELSE SUM(conversions_total) OVER (PARTITION BY user_pseudo_id) / COUNT       
            (collected_source) OVER (PARTITION BY 
            user_pseudo_id) END AS value

            FROM collected_sessions  
            WHERE window_diff BETWEEN 0 AND 604800000000)

SELECT IFNULL(collected_source, 'none'), SUM(value) as value FROM sequences GROUP BY 1 HAVING value > 0 ORDER BY 
value DESC
