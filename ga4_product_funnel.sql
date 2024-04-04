#calculate product funnel

WITH

  cte1 AS (
  SELECT DISTINCT
    user_pseudo_id,
    event_name,
    items.item_name #combine users and items
  FROM `ga4-api-390311.analytics_337075038.events_20240402`,
    UNNEST(items) AS items
    WHERE event_name IN ('view_item', 'add_to_cart', 'begin_checkout', 'purchase') #filter ecommerce events
    AND user_pseudo_id IN 
    (
      SELECT DISTINCT user_pseudo_id FROM `ga4-api-390311.analytics_337075038.events_20240402`   
      WHERE (SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_number') = 1 #take only new users
    ))

SELECT
  item_name,
  COUNT (DISTINCT CASE WHEN event_name = 'view_item' THEN user_pseudo_id ELSE NULL END) AS step1_view_item, #calculate steps
  COUNT (DISTINCT CASE WHEN event_name = 'add_to_cart' THEN user_pseudo_id ELSE NULL END) AS step2_add_to_cart,
  COUNT (DISTINCT CASE WHEN event_name = 'begin_checkout' THEN user_pseudo_id ELSE NULL END) AS step3_begin_checkout,
  COUNT (DISTINCT CASE WHEN event_name = 'purchase' THEN user_pseudo_id ELSE NULL END) AS step4_purchase
FROM
  cte1
GROUP BY
  item_name
