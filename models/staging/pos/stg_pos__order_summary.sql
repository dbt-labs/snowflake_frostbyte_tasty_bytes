with order_detail as (

    select * from {{ ref('base_pos__order_details') }}

),
order_header as (

    select * from {{ ref('base_pos__order_headers') }}
    
),
truck as (

    select * from {{ ref('base_pos__trucks') }}

), 
menu as (

    select * from {{ ref('base_pos__menus') }}

),

final as (

    select order_header.order_id,
           order_header.truck_id, 
           order_header.order_ts,
           order_header.order_amount,
           order_header.order_tax_amount,
           order_header.order_discount_amount,
           order_header.order_total,
           order_header.location_id,
           order_header.customer_id,
           order_detail.order_detail_id, 
           order_detail.line_number,
           order_detail.menu_item_id,
           order_detail.quantity,
           order_detail.unit_price,
           order_detail.price,
           truck.primary_city,
           truck.region,
           truck.country,
           truck.franchise_flag,
           truck.franchise_id,
           menu.truck_brand_name,
           menu.menu_type,
           menu.menu_item_name,
           menu.cost_of_goods_usd
      from order_header
      join order_detail
        on order_header.order_id = order_detail.order_id
      join truck
        on order_header.truck_id = truck.truck_id
      join menu
        on order_detail.menu_item_id = menu.menu_item_id
)

select *
  from final