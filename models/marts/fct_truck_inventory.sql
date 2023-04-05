with in_out_detail as
(   
    select date,
           truck_id,
           item_id,
           item_name,
           unit_price,
           unit,
           category,
           shelf_life_days,
           po_id,
           quantity,
           expiration_date,
           'IN' as status
      from {{ ref('int_truck_inbound') }}
     where po_id is not null

    union

    select date,
           truck_id,
           item_id,
           item_name,
           unit_price,
           unit,
           category,
           shelf_life_days,
           po_id,
           (quantity * -1) as quantity,
           expiration_date,
           'OUT' as status
from {{ ref('int_truck_item_usage') }} 

)

    select date,
           truck_id,
           item_id,
           item_name,
           unit_price,
           unit,
           category,
           shelf_life_days,
           po_id,
           expiration_date,
           round(sum(quantity),3) as quantity_remaining,
           case when quantity_remaining <= 0 
                then 'Out of Stock'
                when quantity_remaining between 0 and 1 
                then 'Low Stock'
                else 'In Stock'
            end as inventory_status,
           case when quantity_remaining > 0 
                then (sum(quantity) * unit_price)
                else null
            end as cost_of_inventory
      from in_out_detail
     group by date, 
              truck_id, 
              item_id, 
              item_name, 
              unit_price, 
              unit,
              category, 
              shelf_life_days, 
              po_id, 
              expiration_date
    having (quantity_remaining > 0 and date < expiration_date)