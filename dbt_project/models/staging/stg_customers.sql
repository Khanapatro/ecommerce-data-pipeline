with source as (
    select * from silver.customers
),

renamed as (
    select
        customer_id,
        full_name,
        first_name,
        last_name,
        email,
        phone,
        gender,
        segment,
        is_active,
        address_city                                    as city,
        address_country                                 as country,
        address_state                                   as state,
        cast(signup_date as date)                       as signup_date,
        cast(date_of_birth as string)                   as date_of_birth,
        _silver_timestamp
    from source
    where customer_id is not null
)

select * from renamed