with in_out_detail as
(   
    select *
      from {{ ref('int_truck_inventory_summary') }}

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
           quantity,
           status
      from in_out_detail
      where created_date < expiration_date