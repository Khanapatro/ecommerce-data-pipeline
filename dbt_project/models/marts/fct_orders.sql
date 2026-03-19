with base as (
    select * from {{ ref('int_orders_payments') }}
)

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
    total_amount,
    order_status,
    payment_method,
    payment_status,
    gateway,
    payment_amount,
    processing_time_ms,
    channel,
    promo_code,
    currency,
    date(order_date)                                    as order_date,
    year(order_date)                                    as order_year,
    month(order_date)                                   as order_month,
    dayofmonth(order_date)                              as order_day
from base