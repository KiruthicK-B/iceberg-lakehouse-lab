SELECT
    sku,
    product_title,
    product_type,
    price,
    vendor
FROM {{ source('bronze_shopify', 'shopify_items_raw') }}
