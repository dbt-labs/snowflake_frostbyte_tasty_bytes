with distribution_summary as (

    select * from {{ ref('stg_supply_chain__distribution_summary') }}

),

eod_stock_assignment as (

    select * from {{ ref('stg_supply_chain__eod_stock_assignments') }}

),

item as (

    select * from {{ ref('stg_supply_chain__items') }}

),

inbound as (

    select distribution_summary.distribution_date,
           distribution_summary.truck_id,
           distribution_summary.expiration_date,
           distribution_summary.po_id,
           distribution_summary.quantity,
           item.item_id,
           item.item_name,
           item.unit,
           item.unit_price,
           item.item_category,
           item.shelf_life_days,
           'IN' as status
      from distribution_summary
      join item
        on item.item_id = distribution_summary.item_id

),

item_usage as (

    select eod_stock_assignment.created_date,
           eod_stock_assignment.truck_id,
           eod_stock_assignment.expiration_date,
           eod_stock_assignment.po_id,
           (eod_stock_assignment.quantity * -1) as quantity,
           item.item_id,
           item.item_name,
           item.unit,
           item.unit_price,
           item.item_category,
           item.shelf_life_days,
           'OUT' as status
  from eod_stock_assignment 
  join item
    on eod_stock_assignment.item_id = item.item_id

)

select dateadd('day', - 1, distribution_date) as created_date,
       expiration_date,
       truck_id,
       po_id,
       quantity,
       item_id,
       item_name,
       unit,
       unit_price,
       item_category,
       shelf_life_days,
       status
  from inbound

  union all

select created_date,
       expiration_date,
       truck_id,
       po_id,
       quantity,
       item_id,
       item_name,
       unit,
       unit_price,
       item_category,
       shelf_life_days,
       status
  from item_usage