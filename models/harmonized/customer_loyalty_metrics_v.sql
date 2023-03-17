with
    customer_metrics as (
        select
            cl.customer_id,
            cl.city,
            cl.country,
            cl.first_name,
            cl.last_name,
            cl.phone_number,
            cl.e_mail,
            sum(oh.order_total) as total_sales,
            array_agg(distinct oh.location_id) as visited_location_ids_array
        from {{ ref("stg_customer_loyalty") }} cl
        join {{ ref("stg_order_header") }} oh on cl.customer_id = oh.customer_id
        group by
            cl.customer_id,
            cl.city,
            cl.country,
            cl.first_name,
            cl.last_name,
            cl.phone_number,
            cl.e_mail
    )
select *
from customer_metrics
