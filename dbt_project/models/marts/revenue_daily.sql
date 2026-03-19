with orders as (
    select * from {{ ref('fct_orders') }}
)

select
    order_date,
    order_year,
    order_month,
    count(order_id)                                         as total_orders,
    sum(total_amount)                                       as total_revenue,
    avg(total_amount)                                       as avg_order_value,
    sum(tax)                                                as total_tax,
    sum(shipping_cost)                                      as total_shipping,
    sum(subtotal)                                           as total_subtotal,
    sum(case when payment_status = 'success' then 1 else 0 end)  as successful_payments,
    sum(case when payment_status = 'failed'  then 1 else 0 end)  as failed_payments
from orders
group by order_date, order_year, order_month
order by order_date