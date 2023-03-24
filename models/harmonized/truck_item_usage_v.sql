WITH _all_items_and_trucks AS 
(
SELECT DISTINCT
    t.*,
    i.*
FROM {{ ref('stg_truck') }} t
JOIN {{ ref('stg_menu') }} m
    ON m.menu_type_id = t.menu_type_id
JOIN {{ ref('stg_recipe') }} r
	ON r.menu_item_id = m.menu_item_id
JOIN {{ ref('stg_item') }} i
	ON i.item_id = r.item_id
),

_all_items_trucks_and_dates AS
(
SELECT 
    dd.date,
    aitad.truck_id,
    aitad.item_id,
    aitad.* EXCLUDE (truck_id, item_id)
FROM _all_items_and_trucks aitad
JOIN {{ ref('stg_dim_date') }} dd
WHERE dd.date <= CURRENT_DATE()
),

_truck_item_id_usage AS
(
SELECT 
    oh.truck_id,
	DATE(oh.order_ts) AS date,
	i.item_id,
	SUM(r.unit_quantity) AS quantity_used
FROM {{ ref('stg_order_header') }} oh
JOIN {{ ref('stg_order_detail') }} od
	ON oh.order_id = od.order_id
JOIN {{ ref('stg_recipe') }} r
	ON r.menu_item_id = od.menu_item_id
JOIN {{ ref('stg_item') }} i
	ON i.item_id = r.item_id
WHERE DATE(oh.order_ts) >= '10/16/2022' -- first day we have inventory metrics in sample data
GROUP BY oh.truck_id, date, i.item_id
)
SELECT 
    aitad.date,
    aitad.truck_id,
    aitad.item_id,
    ZEROIFNULL((tiiu.quantity_used * -1)) AS quantity_used,
    aitad.* EXCLUDE (date, truck_id, item_id)
FROM _all_items_trucks_and_dates aitad
LEFT JOIN _truck_item_id_usage tiiu
    ON aitad.date = tiiu.date
    AND aitad.truck_id = tiiu.truck_id
    AND aitad.item_id = tiiu.item_id