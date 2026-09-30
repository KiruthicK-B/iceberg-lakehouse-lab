-- "Enterprise silver" relationship layer: only keep orders whose FKs actually
-- resolve against this platform's own users/items -- referential integrity
-- enforced here, within Amazon only, before Gold is built on top.
SELECT o.*
FROM {{ ref('stg_amazon__orders') }} o
WHERE o.customer_id IN (SELECT customer_id FROM {{ ref('ent_amazon__users') }})
  AND o.asin IN (SELECT asin FROM {{ ref('ent_amazon__items') }})
