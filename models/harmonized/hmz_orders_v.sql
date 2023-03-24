
with order_detail as (

    select * from {{ ref('stg_order_detail') }}

),
order_header as (

    select * from {{ ref('stg_order_header') }}
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
    select 
        oh.order_id,
        oh.truck_id,
        oh.order_ts,
        od.order_detail_id,
        od.line_number,
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
        od.menu_item_id,
        m.menu_item_name,
        od.quantity,
        od.unit_price,
        od.price,
        oh.order_amount,
        oh.order_tax_amount,
        oh.order_discount_amount,
        oh.order_total
            from order_detail od
            join order_header oh
                on od.order_id = oh.order_id
            join truck t
                on oh.truck_id = t.truck_id
            join menu m
                on od.menu_item_id = m.menu_item_id
            join franchise f
                on t.franchise_id = f.franchise_id
            join location l
                on oh.location_id = l.location_id
            left join customer_loyalty cl
                on oh.customer_id = cl.customer_id
)
select 
    *
from
    final
order by
    order_id,truck_id