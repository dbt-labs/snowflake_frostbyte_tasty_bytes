{{
  config(
    materialized = 'table'
    )
}}

with date_spine as ( 
{{ dbt_utils.date_spine(
    datepart="day",
    start_date="to_date('2020/01/01', 'yyyy/mm/dd')",
    end_date="to_date('2027/01/01', 'yyyy/mm/dd')"
   )
}}  
)

select date_day as date
   from date_spine