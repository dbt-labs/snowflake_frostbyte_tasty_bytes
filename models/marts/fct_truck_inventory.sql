SELECT 
    ti.date,
    ti.truck_id,
    ti.item_id,
    ti.quantity_added,
    ti.expiration_date AS quantity_added_expiration_date,
    tiu.quantity_used,
    CASE WHEN (SUM(ti.quantity_added + tiu.quantity_used) OVER (PARTITION BY ti.truck_id, ti.item_id ORDER BY ti.date)) < 0 THEN 0
    ELSE SUM(ti.quantity_added + tiu.quantity_used) OVER (PARTITION BY ti.truck_id, ti.item_id ORDER BY ti.date)
    END AS quantity_on_truck,
    CASE 
        WHEN quantity_on_truck <= 0 THEN 'Out of Stock'
        WHEN quantity_on_truck BETWEEN 0 AND 1 THEN 'Low Stock'
        ELSE 'In Stock' 
    END AS inventory_status,
    (quantity_on_truck * ti.unit_price) AS cost_of_inventory,
    ti.* EXCLUDE (date, truck_id, item_id, quantity_added, expiration_date, image_url)
FROM {{ ref('int_truck_inbound') }} ti
JOIN {{ ref('int_truck_item_usage') }} tiu
    ON ti.date = tiu.date
    AND ti.truck_id = tiu.truck_id
    AND ti.item_id = tiu.item_id
ORDER BY ti.truck_id, ti.date, ti.item_id