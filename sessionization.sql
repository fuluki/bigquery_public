#draw the session table

WITH hits AS (SELECT user_pseudo_id, 
       CONCAT(user_pseudo_id, (SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id')) AS session_id, #unique session id
       event_date,
       event_timestamp, 
       device.category AS device_category,
       geo.country AS country,
       IFNULL(collected_traffic_source.manual_source, 'none') AS session_source, #traffic source
       IF(event_name = 'form_send', 1, 0) AS is_conversion
       FROM `****` 
       WHERE event_name NOT IN ('session_start', 'first_visit')), #exclude service events

       window_hits AS (SELECT * EXCEPT(is_conversion, event_timestamp),
       ROW_NUMBER() OVER (PARTITION BY session_id ORDER BY event_timestamp) AS hit_order, #numbering the hits
       FORMAT_TIMESTAMP('%d/%m/%Y %H:%M', TIMESTAMP_MICROS(event_timestamp)) AS session_start,

       ROUND((MAX(event_timestamp) OVER (PARTITION BY session_id) - MIN(event_timestamp) OVER (PARTITION BY session_id)) / 1000000,  
       1) AS session_length_in_sec, #calculate session duration
       SUM(is_conversion) OVER (PARTITION BY session_id) AS conversion_total FROM hits)

       SELECT * EXCEPT(hit_order) FROM window_hits WHERE hit_order = 1 #inherit all the dimesions from the 1st hit
