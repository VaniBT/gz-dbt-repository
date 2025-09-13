SELECT (s.quantity * p.purchase_price) AS purchase_cost,
round((s.revenue - (s.quantity * p.purchase_price)),2) AS margin
FROM {{ ref ('stg_raw__sales')}} AS s
INNER JOIN {{ ref ('stg_raw__product')}} AS p
ON s.products_id = p.products_id