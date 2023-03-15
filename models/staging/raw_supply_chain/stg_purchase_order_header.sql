{{ config(materialized="table", sort="po_id", dist="po_id") }}

with
    source as (
        select
            po_id,
            vendor_id,
            shipment_id,
            po_date,
            po_total,
            warehouse_id,
            shipment_date,
            tracking_id,
            expected_date,
            delivery_date,
            po_status,
            created_date,
            updated_date
        from {{ source("raw_supply_chain", "purchase_order_header") }}
    ),
    renamed as (
        select
            po_id as po_id,
            vendor_id as vendor_id,
            shipment_id as shipment_id,
            po_date as po_date,
            po_total as po_total,
            warehouse_id as warehouse_id,
            shipment_date as shipment_date,
            tracking_id as tracking_id,
            expected_date as expected_date,
            delivery_date as delivery_date,
            po_status as po_status,
            created_date as created_date,
            updated_date as updated_date
        from source
    )
select *
from renamed
