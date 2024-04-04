#calculate retention by cohorts

WITH cohorts AS (SELECT DISTINCT user_pseudo_id, 
       MIN(event_date) OVER (PARTITION BY user_pseudo_id) AS cohort_date,
       MAX(IF(event_name='form_send', event_date, NULL)) OVER (PARTITION BY user_pseudo_id) AS max_conversion_date
FROM `project_id`)
SELECT cohort_date, 
       COUNT(user_pseudo_id) AS users, 
       COUNT(max_conversion_date) AS converted_users,
       ROUND(COUNT(max_conversion_date) / COUNT(user_pseudo_id), 2) AS cr
       FROM cohorts
GROUP BY cohort_date
ORDER BY cohort_date
