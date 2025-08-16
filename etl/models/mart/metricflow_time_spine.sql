with date_dim as (


    {{ dbt_date.get_date_dimension("2016-01-01", "2017-12-31") }}

)
select 
    *
from date_dim
