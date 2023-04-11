
with
    source as (
        select *
          from {{ source("raw_pos", "order_header") }}
    ),

    renamed as (
        select
            order_id as order_id,
            truck_id as truck_id,
            location_id as location_id,
            customer_id as customer_id,
            discount_id as discount_id,
            shift_id as shift_id,
            shift_start_time as shift_start_time,
            shift_end_time as shift_end_time,
            order_channel as order_channel,
            order_ts as order_ts,
            served_ts as served_ts,
            order_currency as order_currency,
            order_amount as order_amount,
            order_tax_amount as order_tax_amount,
            order_discount_amount as order_discount_amount,
            order_total as order_total
        from source
    )
    
select *
from renamed
