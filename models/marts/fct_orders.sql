{{
    config(
        materialized = "table",
        transient = false
    )
}}

with holiday as (

    select * from {{ ref('dim_holidays') }}

),
orders as (

    select * from {{ ref('int_orders') }}

),
safegraph as (

    select * from {{ ref('dim_safegraph_poi') }}

),
final as (

    select date(orders.order_ts) as date,
           orders.order_id,
           orders.truck_id,
           orders.order_ts,
           orders.order_detail_id,
           orders.line_number,
           orders.quantity,
           orders.unit_price,
           orders.price,
           orders.menu_item_id,
           orders.order_amount,
           orders.order_tax_amount,
           orders.order_discount_amount,
           orders.order_total,
           orders.truck_brand_name,
           orders.menu_type,
           orders.menu_item_name,
           orders.primary_city,
           orders.region,
           orders.franchise_flag,
           orders.franchise_id,
           orders.franchisee_first_name,
           orders.franchisee_last_name,
           orders.location_id,
           orders.customer_id,
           orders.first_name,
           orders.last_name,
           orders.e_mail,
           orders.phone_number,
           orders.children_count,
           orders.gender,
           orders.marital_status,
           holiday.holiday_name,
           zeroifnull(holiday.holiday_flag) as holiday_flag,
           safegraph.placekey,
           safegraph.location_name,
           safegraph.latitude,
           safegraph.longitude,
           safegraph.top_category,
           safegraph.sub_category,
           safegraph.street_address,
           safegraph.city,
           safegraph.country,
           safegraph.iso_country_code
      from orders
      join safegraph
        on safegraph.location_id = orders.location_id
       and safegraph.city = orders.primary_city
       and safegraph.country = orders.country
      left join holiday
        on holiday.date = date(orders.order_ts)
       and holiday.country = orders.country
)
select 
    *
from
    final
