{{ config(materialized="view", sort="truck_id", dist="truck_id") }}

with
    source as (
        select
            truck_id,
            forecast_date,
            shift_id,
            location_id,
            shift_forecast,
            menu_type_id,
            created_date,
            updated_date
        from {{ source("raw_truck", "truck_shift") }}
    ),
    renamed as (
        select
            truck_id as truck_id,
            forecast_date as forecast_date,
            shift_id as shift_id,
            location_id as location_id,
            shift_forecast as shift_forecast,
            menu_type_id as menu_type_id,
            created_date as created_date,
            updated_date as updated_date
        from source
    )
select *
from renamed
