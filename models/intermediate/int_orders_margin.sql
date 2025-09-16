SELECT s.orders_id,
    s.date_date,
    ROUND(SUM(s.revenue),2) AS revenue,
    SUM(s.quantity) AS quantity,
    SUM((s.quantity * p.purchase_price)) AS purchase_cost,
    ROUND(SUM(s.revenue - (s.quantity * p.purchase_price)), 2) AS margin
FROM {{ ref ('stg_raw__sales')}} AS s
INNER JOIN {{ ref ('stg_raw__product')}} AS p
ON s.products_id = p.products_id
GROUP BY s.orders_id, date_date
ORDER BY s.orders_id DESC
