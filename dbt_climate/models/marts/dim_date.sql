with timeline as (
    select distinct observation_date as date_day
    from {{ ref('stg_observations') }}
)

select
    date_day,
    date_part(year,  date_day)::int as year,
    date_part(month, date_day)::int as month,
    date_part(day,   date_day)::int as day,
    case when date_part(month, date_day) between 3 and 5 then 'spring'
         when date_part(month, date_day) between 6 and 8 then 'summer'
         when date_part(month, date_day) between 9 and 11 then 'fall'
         else 'winter' end as season
from timeline