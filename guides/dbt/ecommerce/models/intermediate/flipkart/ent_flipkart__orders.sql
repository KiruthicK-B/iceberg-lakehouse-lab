SELECT o.*
FROM {{ ref('stg_flipkart__orders') }} o
WHERE o.user_id IN (SELECT user_id FROM {{ ref('ent_flipkart__users') }})
  AND o.product_id IN (SELECT product_id FROM {{ ref('ent_flipkart__items') }})
