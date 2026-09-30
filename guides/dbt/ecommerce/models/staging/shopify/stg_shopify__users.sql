-- Native Shopify field names kept throughout -- no renaming to a shared
-- cross-platform vocabulary.
SELECT
    cust_id,
    cust_email,
    display_name,
    CAST(to_timestamp(created_at, "yyyy-MM-dd'T'HH:mm:ss'Z'") AS DATE) AS signup_date,
    country_code
FROM {{ source('bronze_shopify', 'shopify_users_raw') }}
