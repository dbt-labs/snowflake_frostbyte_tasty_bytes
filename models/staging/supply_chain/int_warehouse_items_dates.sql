
with warehouse as (

    select * from {{ ref('stg_supply_chain__warehouses') }}

),
item as (

    select * from {{ ref('stg_supply_chain__items') }}

),
vendor as (

    select * from {{ ref('stg_supply_chain__vendors') }}

),
dates as (

    select * from {{ ref('all_days') }}

),

all_warehouses_all_items as (

    select warehouse.warehouse_id,
           warehouse.warehouse_name,
           warehouse.warehouse_city,
           warehouse.warehouse_country,
           warehouse.warehouse_address,
           warehouse.warehouse_latitude,
           warehouse.warehouse_longitude,
           item.item_id,
           item.item_name,
           item.item_category,
           item.unit,
           item.unit_price,
           item.unit_currency,
           item.shelf_life_days,
           item.vendor_id,
           item.image_url,
           vendor.vendor_name,
           vendor.vendor_category 
      from warehouse
      join item
      join vendor
        on item.item_category = vendor.vendor_category

),

all_warehouses_all_items_all_dates as
(
    select dim_date.date,
           awai.warehouse_id,
           awai.warehouse_name,
           awai.warehouse_city,
           awai.warehouse_country,
           awai.warehouse_address,
           awai.warehouse_latitude,
           awai.warehouse_longitude,
           awai.item_id,
           awai.item_name,
           awai.item_category,
           awai.vendor_category,
           awai.unit,
           awai.unit_price,
           awai.unit_currency,
           awai.shelf_life_days,
           awai.image_url,
           awai.vendor_id,
           awai.vendor_name
      from {{ ref('all_days') }} dim_date
      join all_warehouses_all_items awai
     where dim_date.date >= '10/14/2022'
       and dim_date.date <= CURRENT_DATE()
)

select * from all_warehouses_all_items_all_dates