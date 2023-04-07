with distribution_summary as (

    select * from {{ ref('stg_distribution_summary') }}

),
item as (

    select * from {{ ref('stg_item') }}

),
final as(
    select distribution_summary.distribution_date,
           distribution_summary.truck_id,
           distribution_summary.item_id,
           item.item_name,
           item.unit,
           item.unit_price,
           item.unit_currency,
           item.item_category,
           item.shelf_life_days,
           distribution_summary.expiration_date,
           distribution_summary.po_id,
           distribution_summary.quantity
      from distribution_summary
      join item
        on item.item_id = distribution_summary.item_id
)

select * from final