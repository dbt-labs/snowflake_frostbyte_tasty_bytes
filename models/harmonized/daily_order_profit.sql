{{ config(snowflake_warehouse="tasty_de_wh") }}

select
    order_header_detail.order_ts,
    order_header_detail.order_id,
    order_header_detail.truck_id,
    m.truck_brand_name,
    t.franchise_flag,
    t.primary_city as city,
    t.country,
    sum(order_header_detail.order_total) as sales_usd,
    sum(m.cost_of_goods_usd) as cogs_usd,
    sales_usd - cogs_usd as gross_profit_usd,
    round(((((sales_usd - cogs_usd) / sales_usd)) * 100), 2) as sales_margin_pct
from {{ ref("stg_order_summary") }} order_header_detail
join {{ ref("stg_menu") }} m on m.menu_item_id = order_header_detail.menu_item_id
join {{ ref("stg_truck") }} t on t.truck_id = order_header_detail.truck_id
group by
    order_header_detail.order_ts,
    order_header_detail.order_id,
    order_header_detail.truck_id,
    m.truck_brand_name,
    t.franchise_flag,
    city,
    t.country
order by order_header_detail.order_ts
