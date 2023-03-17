with customer_metrics as (
    SELECT order_id,
           item_id,
           od.ORDER_AMOUNT
        from {{ ref('stg_order_detail') }} od
        join {{ ref('stg_order_header') }} oh   
            on od.order_id = oh.order_id
)
Select * from customer_metrics