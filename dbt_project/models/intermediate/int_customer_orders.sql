with customers as (
    select * from {{ ref('stg_customers') }}
),

orders as (
    select * from {{ ref('stg_orders') }}
),

joined as (
    select
        c.customer_id,
        c.full_name,
        c.email,
        c.phone,
        c.gender,
        c.segment,
        c.city,
        c.country,
        c.signup_date,
        c.is_active,
        count(o.order_id)                               as total_orders,
        sum(o.total_amount)                             as total_spent,
        avg(o.total_amount)                             as avg_order_value,
        min(o.order_date)                               as first_order_date,
        max(o.order_date)                               as last_order_date
    from customers c
    left join orders o
        on c.customer_id = o.customer_id
    group by
        c.customer_id, c.full_name, c.email,
        c.phone, c.gender, c.segment,
        c.city, c.country, c.signup_date, c.is_active
)

select * from joined