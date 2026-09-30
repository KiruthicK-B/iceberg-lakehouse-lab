-- Already FK-clean from the enterprise-silver layer (every customer_id/asin
-- here is guaranteed to exist in dim_amazon_users/dim_amazon_items).
SELECT * FROM {{ ref('ent_amazon__orders') }}
