with tasty_holidays as (

    select * from {{ ref('int_holiday') }}

)
select 
    *
from
    tasty_holidays
order by
    date
