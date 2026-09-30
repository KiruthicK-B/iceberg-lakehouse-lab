SELECT
    order_id,
    customer_id,
    asin,
    CAST(from_unixtime(order_epoch) AS DATE) AS order_date,
    qty,
    ROUND(unit_price_usd * qty, 2) AS amount_usd,
    order_status
FROM {{ source('bronze_amazon', 'amazon_orders_raw') }}
