
with order_summary as (

    select * from {{ ref('stg_order_summary') }}

),
truck as (

    select * from {{ ref('stg_truck') }}

),
menu as (

    select * from {{ ref('stg_menu') }}

),
franchise as (

    select * from {{ ref('stg_franchise') }}

),
location as (

    select * from {{ ref('stg_location') }}

),
customer_loyalty as (

    select * from {{ ref('stg_customer_loyalty') }}

),
final as (
    select order_summary.order_id,
           order_summary.truck_id,
           order_summary.order_ts,
           order_summary.order_detail_id,
           order_summary.line_number,
           order_summary.quantity,
           order_summary.unit_price,
           order_summary.price,
           order_summary.menu_item_id,
           order_summary.order_amount,
           order_summary.order_tax_amount,
           order_summary.order_discount_amount,
           order_summary.order_total,
           menu.truck_brand_name,
           menu.menu_type,
           menu.menu_item_name,
           truck.primary_city,
           truck.region,
           truck.country,
           truck.franchise_flag,
           truck.franchise_id,
           franchise.franchisee_first_name,
           franchise.franchisee_last_name,
           location.location_id,
           customer_loyalty.customer_id,
           customer_loyalty.first_name,
           customer_loyalty.last_name,
           customer_loyalty.e_mail,
           customer_loyalty.phone_number,
           customer_loyalty.children_count,
           customer_loyalty.gender,
           customer_loyalty.marital_status
      from order_summary
      join truck
        on order_summary.truck_id = truck.truck_id
      join menu
        on order_summary.menu_item_id = menu.menu_item_id
      join franchise
        on truck.franchise_id = franchise.franchise_id
      join location
        on order_summary.location_id = location.location_id
      left join customer_loyalty
        on order_summary.customer_id = customer_loyalty.customer_id
)
select 
    *
from
    final
