with order_detail as (

    select * from {{ ref('stg_order_detail') }}

),
order_header as (

    select * from {{ ref('stg_order_header') }}
    
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
           order_detail.price
      from order_header
      join order_detail
        on order_header.order_id = order_detail.order_id
)

select *
  from final