with all_warehouses_all_items_all_dates as
(
    
    select * from {{ ref('int_warehouse_items_dates') }}

),

warehouse_do AS 
(
select warehouse_id,
       dh_id,
       distribution_date,
       line_item_id,
       item_id,
       quantity,
       expiration_date,
       po_id
from {{ ref('stg_supply_chain__distribution_summary') }}
)
select awaiad.date,
       awaiad.warehouse_id,
       awaiad.item_id,
       awaiad.vendor_id,
       warehouse_do.po_id,
       warehouse_do.expiration_date,
       ZEROIFNULL((warehouse_do.quantity * -1)) AS quantity_removed,
       awaiad.warehouse_name,
       awaiad.warehouse_city,
       awaiad.warehouse_country,
       awaiad.warehouse_address,
       awaiad.warehouse_latitude,
       awaiad.warehouse_longitude,
       awaiad.item_name,
       awaiad.item_category,
       awaiad.unit,
       awaiad.unit_price,
       awaiad.unit_currency,
       awaiad.shelf_life_days,
       awaiad.vendor_id,
       awaiad.image_url,
       awaiad.vendor_name,
       awaiad.vendor_category 
  from all_warehouses_all_items_all_dates awaiad
  left join warehouse_do
    on warehouse_do.distribution_date = awaiad.date
   and warehouse_do.warehouse_id = awaiad.warehouse_id
   and warehouse_do.item_id = awaiad.item_id
