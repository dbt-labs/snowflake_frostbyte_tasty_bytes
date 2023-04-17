{{
  config(
    materialized = 'table',
    schema = 'harmonized'
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

select date_day as date,
       year(date_day) as year,
       month(date_day) as month,
       monthname(date_day) as month_name,
       dayofmonth(date_day) as day_of_mon,
       dayofweek(date_day) as day_of_week,
       weekofyear(date_day) as week_of_year,
       dayofyear(date_day) as day_of_year

  from date_spine