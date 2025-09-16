SELECT 
    s.orders_id,
    s.date_date,
    SUM(s.revenue) AS revenue,
    SUM(s.quantity) AS quantity,
    SUM(s.quantity * p.purchase_price) AS purchase_cost,
    ROUND(SUM(s.revenue - (s.quantity * p.purchase_price)), 2) AS margin,
    SUM(sh.shipping_fee) AS shipping_fee,
    SUM(sh.ship_cost) AS ship_cost,
    SUM(sh.logcost) AS log_cost,
    ROUND(
        SUM(s.revenue - (s.quantity * p.purchase_price)) 
        + SUM(sh.shipping_fee) 
        - SUM(sh.logcost) 
        - SUM(sh.ship_cost),
        2
    ) AS operational_margin
FROM {{ ref('stg_raw__sales') }} AS s
INNER JOIN {{ ref('stg_raw__product') }} AS p
    ON s.products_id = p.products_id
INNER JOIN {{ ref('stg_raw__ship') }} AS sh
    ON s.orders_id = sh.orders_id
GROUP BY s.orders_id, s.date_date
ORDER BY s.orders_id DESC

