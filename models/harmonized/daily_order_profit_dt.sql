CREATE OR REPLACE DYNAMIC TABLE frostbyte_tasty_bytes.harmonized.daily_order_profit_dt
LAG = '1440 minutes' -- 24 hours
WAREHOUSE = tasty_de_wh
    AS
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
FROM frostbyte_tasty_bytes.raw_pos.order_header oh
JOIN frostbyte_tasty_bytes.raw_pos.order_detail od
    ON oh.order_id = od.order_id
JOIN frostbyte_tasty_bytes.raw_pos.menu m
    ON m.menu_item_id = od.menu_item_id
JOIN frostbyte_tasty_bytes.raw_pos.truck t
    ON t.truck_id = oh.truck_id
GROUP BY oh.order_ts, oh.order_id, oh.truck_id, m.truck_brand_name, t.franchise_flag, city, t.country
ORDER BY oh.order_ts;