{{ config(materialized="view", sort="dh_detail_id", dist="dh_detail_id") }}

with
    source as (
        select
            dh_detail_id,
            dh_id,
            line_item_id,
            item_id,
            quantity,
            expiration_date,
            po_id,
            created_date,
            updated_date
        from {{ source("raw_supply_chain", "distribution_detail") }}
    ),
    renamed as (
        select
            dh_detail_id as dh_detail_id,
            dh_id as dh_id,
            line_item_id as line_item_id,
            item_id as item_id,
            quantity as quantity,
            expiration_date as expiration_date,
            po_id as po_id,
            created_date as created_date,
            updated_date as updated_date
        from source
    )
select *
from renamed
