SELECT o.*
FROM {{ ref('stg_shopify__orders') }} o
WHERE o.cust_id IN (SELECT cust_id FROM {{ ref('ent_shopify__users') }})
  AND o.sku IN (SELECT sku FROM {{ ref('ent_shopify__items') }})
