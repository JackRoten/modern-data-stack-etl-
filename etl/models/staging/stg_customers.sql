with

source as (

    select * from {{ source('src_postgres', 'customer') }}

),

renamed as (

    select

        ----------  ids
        id as customer_id,

        ---------- text
        name as customer_name

    from source

)

select * from renamed
