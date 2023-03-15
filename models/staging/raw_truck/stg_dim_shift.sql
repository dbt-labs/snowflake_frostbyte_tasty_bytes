{{ config(materialized="table", sort="shift_id", dist="shift_id") }}

with
    source as (
        select shift_id, shift_type, shift_start_time, shift_end_time, pm_shift_id
        from {{ source("raw_truck", "dim_shift") }}
    ),
    renamed as (
        select
            shift_id as shift_id,
            shift_type as shift_type,
            shift_start_time as shift_start_time,
            shift_end_time as shift_end_time,
            pm_shift_id as pm_shift_id
        from source
    )
select *
from renamed
