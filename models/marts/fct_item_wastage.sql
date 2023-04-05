with inventory_log as
(

    select truck_id,
           item_id,
           unit,
           item_name,
           unit_price,
           po_id,
           quantity,
           expiration_date,
           'IN' as direction,
           dateadd('day', - 1, date) as created_date
      from {{ ref('int_truck_inbound') }}

    union all

    select truck_id,
           item_id,
           unit,
           item_name,
           unit_price,
           po_id,
           (quantity * -1) as quantity,
           expiration_date,
           'OUT' as direction,
           date as created_date
      from {{ ref('int_truck_item_usage') }} 

),

inventory_summary as
(

    select inventory_log.truck_id,
           inventory_log.po_id,
           inventory_log.created_date,
           inventory_log.expiration_date,
           inventory_log.item_id,
           inventory_log.item_name,
           inventory_log.unit,
           inventory_log.unit_price,
           round(sum(inv_log1.quantity),2) as available_quantity
      from inventory_log
      join inventory_log inv_log1
        on inventory_log.truck_id = inv_log1.truck_id
       and inventory_log.po_id = inv_log1.po_id
       and inventory_log.item_id = inv_log1.item_id
       and inventory_log.created_date >= inv_log1.created_date
     group by inventory_log.truck_id, 
              inventory_log.item_id, 
              inventory_log.item_name, 
              inventory_log.unit, 
              inventory_log.unit_price, 
              inventory_log.po_id, 
              inventory_log.created_date, 
              inventory_log.expiration_date
)

select *,
       case when created_date = expiration_date and available_quantity > 0 
            then 1
            else 0
        end as is_expired
  from inventory_summary
 where is_expired = 1