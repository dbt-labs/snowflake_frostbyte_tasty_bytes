select warehouse_inbound.date,
       warehouse_inbound.warehouse_id,
       warehouse_inbound.item_id,
       warehouse_inbound.quantity_added,
       warehouse_inbound.po_id,
       warehouse_inbound.expiration_date as quantity_added_expiration_date,
       warehouse_outbound.quantity_removed,
       warehouse_outbound.expiration_date as quantity_removed_expiration_date,
       sum(warehouse_inbound.quantity_added + warehouse_outbound.quantity_removed) 
       over (partition by warehouse_inbound.warehouse_id, warehouse_inbound.item_id order by warehouse_inbound.date) as item_id_quantity_in_stock,
       (item_id_quantity_in_stock * warehouse_inbound.unit_price) as cost_of_inventory,
       warehouse_inbound.warehouse_name,
       warehouse_inbound.warehouse_city,
       warehouse_inbound.warehouse_country,
       warehouse_inbound.warehouse_address,
       warehouse_inbound.warehouse_latitude,
       warehouse_inbound.warehouse_longitude,
       warehouse_inbound.item_name,
       warehouse_inbound.item_category,
       warehouse_inbound.unit,
       warehouse_inbound.unit_price,
       warehouse_inbound.unit_currency,
       warehouse_inbound.shelf_life_days,
       warehouse_inbound.vendor_id,
       warehouse_inbound.image_url,
       warehouse_inbound.vendor_name,
       warehouse_inbound.vendor_category 
  from {{ ref('int_warehouse_inbound') }} warehouse_inbound
  join {{ ref('int_warehouse_outbound') }} warehouse_outbound
    on warehouse_inbound.date = warehouse_outbound.date
   and warehouse_inbound.warehouse_id = warehouse_outbound.warehouse_id
   and warehouse_inbound.item_id = warehouse_outbound.item_id