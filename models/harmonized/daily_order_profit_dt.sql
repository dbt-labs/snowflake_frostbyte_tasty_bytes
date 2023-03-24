{{
    config(
        snowflake_warehouse = "tasty_de_wh"
    )
}}

SELECT 
    oh.order_ts,
    oh.order_id,
    oh.truck_id,
    m.truck_brand_name,
    t.franchise_flag,
    t.primary_city AS city,
    t.country,
    SUM(oh.order_total) AS sales_usd,
    SUM(m.cost_of_goods_usd) AS cogs_usd,
    sales_usd - cogs_usd AS gross_profit_usd,
    ROUND(((((sales_usd - cogs_usd)/sales_usd))*100),2) AS sales_margin_pct
FROM {{ ref('stg_order_header') }} oh
JOIN {{ ref('stg_order_detail') }} od
    ON oh.order_id = od.order_id
JOIN {{ ref('stg_menu') }} m
    ON m.menu_item_id = od.menu_item_id
JOIN {{ ref('stg_truck') }} t
    ON t.truck_id = oh.truck_id
GROUP BY oh.order_ts, oh.order_id, oh.truck_id, m.truck_brand_name, t.franchise_flag, city, t.country
ORDER BY oh.order_ts