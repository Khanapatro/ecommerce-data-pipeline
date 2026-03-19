with source as (
    select * from silver.payments
),

renamed as (
    select
        payment_id,
        order_id,
        customer_id,
        transaction_id,
        payment_method,
        payment_status,
        gateway,
        amount,
        currency,
        card_last_four,
        processing_time_ms,
        failure_reason,
        billing_city,
        billing_country,
        cast(event_time as timestamp)                   as payment_date,
        _silver_timestamp
    from source
    where payment_id is not null
)

select * from renamed