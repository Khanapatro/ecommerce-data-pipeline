with base as (
    select * from {{ ref('stg_inventory') }}
)

select
    product_id,
    product_name,
    category,
    sku,
    supplier_id,
    warehouse_id,
    warehouse_location,
    quantity_on_hand,
    quantity_reserved,
    quantity_available,
    reorder_point,
    unit_cost,
    is_active,
    is_low_stock,
    case
        when quantity_available = 0  then 'out_of_stock'
        when is_low_stock = true     then 'low_stock'
        else 'in_stock'
    end                                                 as stock_status,
    last_restock_date,
    event_time                                          as last_updated
from base