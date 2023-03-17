{{ config(materialized="table", unique_key = "order_id||'-'||line_number", incremental_strategy = 'merge') }}

with
    order_summary as (
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
            -- cpg.placekey,
            -- cpg.location_name,
            -- cpg.top_category,
            -- cpg.sub_category,
            -- cpg.latitude,
            -- cpg.longitude,
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
        from {{ ref("stg_order_detail") }} od
        join {{ ref("stg_order_header") }} oh on od.order_id = oh.order_id
        join {{ ref("stg_truck") }} t on oh.truck_id = t.truck_id
        join {{ ref("stg_menu") }} m on od.menu_item_id = m.menu_item_id
        join {{ ref("stg_franchise") }} f on t.franchise_id = f.franchise_id
        join {{ ref("stg_location") }} l on oh.location_id = l.location_id
        -- JOIN frostbyte_tasty_bytes.raw_safegraph.core_poi_geometry cpg
        -- ON cpg.placekey = l.placekey
        left join
            {{ ref("stg_customer_loyalty") }} cl on oh.customer_id = cl.customer_id
    )
select *
from order_summary
