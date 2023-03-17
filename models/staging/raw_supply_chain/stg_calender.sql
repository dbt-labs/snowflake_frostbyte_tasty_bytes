{{ config(materialized="view", sort="date", dist="date") }}

with
    source as (
        select
            date,
            year,
            month,
            month_name,
            day_of_mon,
            day_of_week,
            week_of_year,
            day_of_year
        from {{ source("raw_supply_chain", "dim_date") }}
    ),
    renamed as (
        select
            date as date,
            year as year,
            month as month,
            month_name as month_name,
            day_of_mon as day_of_mon,
            day_of_week as day_of_week,
            week_of_year as week_of_year,
            day_of_year as day_of_year
        from source
    )
select *
from renamed
