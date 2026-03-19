with orders as (
    select * from {{ ref('stg_orders') }}
),

payments as (
    select * from {{ ref('stg_payments') }}
),

joined as (
    select
        o.order_id,
        o.customer_id,
        o.product_id,
        o.product_name,
        o.category,
        o.quantity,
        o.unit_price,
        o.discount,
        o.line_total,
        o.subtotal,
        o.tax,
        o.shipping_cost,
        o.total_amount,
        o.order_status,
        o.channel,
        o.promo_code,
        o.currency,
        o.order_date,
        p.payment_id,
        p.payment_method,
        p.payment_status,
        p.gateway,
        p.amount                                        as payment_amount,
        p.processing_time_ms,
        p.failure_reason
    from orders o
    left join payments p
        on o.order_id = p.order_id
)

select * from joined