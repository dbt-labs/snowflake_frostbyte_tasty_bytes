with eod_stock_assignment as (

    select * from {{ ref('stg_eod_stock_assignment') }}

),
item as (

    select * from {{ ref('stg_item') }}

)

select eod_stock_assignment.assignment_id,
       eod_stock_assignment.truck_id,
       eod_stock_assignment.po_id,
       eod_stock_assignment.quantity,
       eod_stock_assignment.expiration_date,
       eod_stock_assignment.created_date,
       item.item_id,
       item.item_name,
       item.unit_price,
       item.item_category,
       item.unit,
       item.shelf_life_days
  from eod_stock_assignment 
  join item
    on eod_stock_assignment.item_id = item.item_id