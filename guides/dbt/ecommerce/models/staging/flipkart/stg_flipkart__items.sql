-- Kept in native INR -- no USD conversion, nothing gets compared cross-platform.
SELECT
    product_id,
    product_name,
    category,
    mrp_inr,
    seller
FROM {{ source('bronze_flipkart', 'flipkart_items_raw') }}
