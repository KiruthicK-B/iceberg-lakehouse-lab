SELECT
    asin,
    title,
    category,
    price_usd,
    brand
FROM {{ source('bronze_amazon', 'amazon_items_raw') }}
