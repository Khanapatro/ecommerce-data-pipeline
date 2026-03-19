with base as (
    select * from {{ ref('int_customer_orders') }}
)

select
    customer_id,
    full_name,
    email,
    phone,
    gender,
    segment,
    city,
    country,
    signup_date,
    is_active,
    total_orders,
    total_spent,
    avg_order_value,
    first_order_date,
    last_order_date,
    case
        when total_spent >= 1000 then 'high_value'
        when total_spent >= 500  then 'mid_value'
        else 'low_value'
    end                                                 as customer_tier,
    datediff(current_date(), signup_date)               as days_as_customer
from base