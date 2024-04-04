#calculate open funnel

SELECT COUNT(DISTINCT IF(event_name='session_start', user_pseudo_id, NULL)) AS session_start,
       COUNT(DISTINCT IF(event_name='scroll', user_pseudo_id, NULL)) AS scroll,
       COUNT(DISTINCT IF(event_name='form_send', user_pseudo_id, NULL)) AS form_send
FROM `project_id` 
