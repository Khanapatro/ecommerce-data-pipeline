with source as (
    select * from silver.orders
),

renamed as (
    select
        order_id,
        customer_id,
        product_id,
        product_name,
        category,
        quantity,
        unit_price,
        discount,
        line_total,
        subtotal,
        tax,
        shipping_cost,
        shipping_method,
        order_total                                     as total_amount,
        status                                          as order_status,
        channel,
        promo_code,
        currency,
        item_count,
        shipping_city,
        shipping_country,
        shipping_state,
        cast(event_time as timestamp)                   as order_date,
        _silver_timestamp
    from source
    where order_id is not null
)

select * from renamed