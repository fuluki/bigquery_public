WITH hits AS (

       SELECT user_pseudo_id, 
              CONCAT(user_pseudo_id, (SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id')) AS session_id,
              event_timestamp,
              IFNULL(collected_traffic_source.manual_medium, 'none') AS collected_medium,
              IF(event_name = 'form_send', 1, 0) AS is_conversion  
        FROM `project_id` 
        WHERE event_name NOT IN ('session_start', 'user_engagement', 'first_visit')),

     window_hits AS (
        SELECT * EXCEPT(is_conversion), ROW_NUMBER() OVER (PARTITION BY session_id ORDER BY event_timestamp) AS hit_number,
        SUM(is_conversion) OVER (PARTITION BY session_id) AS conversions_total                     
        FROM hits)

SELECT * EXCEPT(hit_number) FROM window_hits WHERE hit_number = 1
ORDER BY user_pseudo_id, event_timestamp
