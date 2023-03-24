SELECT 
    wi.date,
    wi.warehouse_id,
    wi.item_id,
    wi.quantity_added,
    wi.po_id,
    wi.expiration_date AS quantity_added_expiration_date,
    wo.quantity_removed,
    wo.expiration_date AS quantity_removed_expiration_date,
    SUM(wi.quantity_added + wo.quantity_removed) OVER (PARTITION BY wi.warehouse_id, wi.item_id ORDER BY wi.date)
        AS item_id_quantity_in_stock,
    (item_id_quantity_in_stock * wi.unit_price) AS cost_of_inventory,
    wi.* EXCLUDE (date, warehouse_id, item_id, quantity_added, expiration_date, po_id)
FROM {{ ref('warehouse_inbound_v') }} wi
JOIN {{ ref('warehouse_outbound_v') }} wo
    ON wi.date = wo.date
    AND wi.warehouse_id = wo.warehouse_id
    AND wi.item_id = wo.item_id
ORDER BY wi.warehouse_id, wi.date, wi.item_id