with all_warehouses_all_items_all_dates as
(
    
    select * from {{ ref('int_warehouse_items_dates') }}

),

_warehouse_do AS 
(
SELECT 
  warehouse_id,
  dh_id,
  distribution_date,
  line_item_id,
  item_id,
  quantity,
  expiration_date,
  po_id
FROM {{ ref('stg_distribution_summary') }}
)
SELECT 
    awaiad.date,
    awaiad.warehouse_id,
    awaiad.item_id,
    awaiad.vendor_id,
    wd.po_id,
    wd.expiration_date,
    ZEROIFNULL((wd.quantity * -1)) AS quantity_removed,
    awaiad.* EXCLUDE (date, warehouse_id, item_id, vendor_id)
FROM _all_warehouses_all_items_all_dates awaiad
LEFT JOIN _warehouse_do wd
    ON wd.distribution_date = awaiad.date
    AND wd.warehouse_id = awaiad.warehouse_id
    AND wd.item_id = awaiad.item_id
