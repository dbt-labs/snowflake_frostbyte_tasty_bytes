{config(materialized="view"}

CREATE OR REPLACE VIEW frostbyte_tasty_bytes.harmonized.truck_item_usage_v
COMMENT = 'Truck Item_ID Level Inventory Daily Usage View'
    AS
WITH _all_items_and_trucks AS 
(
SELECT DISTINCT
    t.*,
    i.*
FROM frostbyte_tasty_bytes.raw_pos.truck t
JOIN frostbyte_tasty_bytes.raw_pos.menu m
    ON m.menu_type_id = t.menu_type_id
JOIN frostbyte_tasty_bytes.raw_supply_chain.recipe r
	ON r.menu_item_id = m.menu_item_id
JOIN frostbyte_tasty_bytes.raw_supply_chain.item i
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
JOIN frostbyte_tasty_bytes.raw_supply_chain.dim_date dd
WHERE 1=1
    AND dd.date <= CURRENT_DATE()
),
_truck_item_id_usage AS
(
SELECT 
    oh.truck_id,
	DATE(oh.order_ts) AS date,
	i.item_id,
	SUM(r.unit_quantity) AS quantity_used
FROM frostbyte_tasty_bytes.raw_pos.order_header oh
JOIN frostbyte_tasty_bytes.raw_pos.order_detail od
	ON oh.order_id = od.order_id
JOIN frostbyte_tasty_bytes.raw_supply_chain.recipe r
	ON r.menu_item_id = od.menu_item_id
JOIN frostbyte_tasty_bytes.raw_supply_chain.item i
	ON i.item_id = r.item_id
WHERE 1=1
    AND DATE(oh.order_ts) >= '10/16/2022' -- first day we have inventory metrics in sample data
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
    AND aitad.item_id = tiiu.item_id;