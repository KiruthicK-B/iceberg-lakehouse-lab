-- Native Flipkart field names kept throughout -- no renaming to a shared
-- cross-platform vocabulary.
SELECT
    user_id,
    name,
    email,
    CAST(join_date AS DATE) AS signup_date,
    state
FROM {{ source('bronze_flipkart', 'flipkart_users_raw') }}
