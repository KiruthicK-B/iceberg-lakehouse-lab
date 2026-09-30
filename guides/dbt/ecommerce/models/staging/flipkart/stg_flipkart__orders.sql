SELECT
    order_no,
    user_id,
    product_id,
    CAST(order_date AS DATE) AS order_date,
    quantity,
    amount_inr,
    status
FROM {{ source('bronze_flipkart', 'flipkart_orders_raw') }}
