with source as (
    select * from silver.inventory
),

renamed as (
    select
        inventory_id,
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
        reorder_quantity,
        unit_cost,
        is_active,
        is_low_stock,
        cast(last_restock_date as string)               as last_restock_date,
        cast(event_time as timestamp)                   as event_time,
        _silver_timestamp
    from source
    where product_id is not null
)

select * from renamed