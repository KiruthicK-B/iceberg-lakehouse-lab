-- Native Amazon field names kept throughout -- no renaming to a shared
-- cross-platform vocabulary. Only transformation: epoch -> DATE.
SELECT
    customer_id,
    full_name,
    email_address,
    CAST(from_unixtime(signup_epoch) AS DATE) AS signup_date,
    country
FROM {{ source('bronze_amazon', 'amazon_users_raw') }}
