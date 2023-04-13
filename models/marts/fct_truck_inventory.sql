with in_out_detail as
(   
    select *
      from {{ ref('int_truck_inbound_item_usage') }}

)

    select created_date,
           truck_id,
           item_id,
           item_name,
           unit_price,
           unit,
           item_category,
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
     group by created_date, 
              truck_id, 
              item_id, 
              item_name, 
              unit_price, 
              unit,
              item_category, 
              shelf_life_days, 
              po_id, 
              expiration_date
    having (quantity_remaining > 0 and created_date < expiration_date)