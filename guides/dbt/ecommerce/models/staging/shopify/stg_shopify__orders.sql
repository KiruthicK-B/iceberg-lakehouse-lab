SELECT
    shopify_order_id,
    cust_id,
    sku,
    CAST(to_timestamp(created_at, "yyyy-MM-dd'T'HH:mm:ss'Z'") AS DATE) AS order_date,
    line_qty,
    ROUND(total_price, 2) AS amount_usd,
    financial_status
FROM {{ source('bronze_shopify', 'shopify_orders_raw') }}
