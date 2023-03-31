with tasty_holidays as (

    select * from {{ ref('hmz_holiday_v') }}

)
select 
    *
from
    tasty_holidays
order by
    date
