
with order_header_detail as (

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
    select order_header_detail.order_id,
           order_header_detail.truck_id,
           order_header_detail.order_ts,
           order_header_detail.order_detail_id,
           order_header_detail.line_number,
           order_header_detail.quantity,
           order_header_detail.unit_price,
           order_header_detail.price,
           order_header_detail.menu_item_id,
           order_header_detail.order_amount,
           order_header_detail.order_tax_amount,
           order_header_detail.order_discount_amount,
           order_header_detail.order_total,
           m.truck_brand_name,
           m.menu_type,
           t.primary_city,
           t.region,
           t.country,
           t.franchise_flag,
           t.franchise_id,
           f.first_name as franchisee_first_name,
           f.last_name as franchisee_last_name,
           l.location_id,
           cl.customer_id,
           cl.first_name,
           cl.last_name,
           cl.e_mail,
           cl.phone_number,
           cl.children_count,
           cl.gender,
           cl.marital_status,
           m.menu_item_name
      from order_header_detail
      join truck t
        on order_header_detail.truck_id = t.truck_id
      join menu m
        on order_header_detail.menu_item_id = m.menu_item_id
      join franchise f
        on t.franchise_id = f.franchise_id
      join location l
        on order_header_detail.location_id = l.location_id
      left join customer_loyalty cl
        on order_header_detail.customer_id = cl.customer_id
)
select 
    *
from
    final
