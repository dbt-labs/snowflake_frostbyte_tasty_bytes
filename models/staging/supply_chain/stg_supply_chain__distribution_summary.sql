with distribution_header as (

    select * from {{ ref('base_supply_chain__distribution_headers') }}

),
distribution_detail as (

    select * from {{ ref('base_supply_chain__distribution_details') }}
    
),

final as (

    select distribution_header.dh_id,
           distribution_header.distribution_date,
           distribution_header.truck_id,
           distribution_header.warehouse_id, 
           distribution_detail.item_id,
           distribution_detail.expiration_date,
           distribution_detail.po_id,
           distribution_detail.quantity,
           distribution_detail.line_item_id
      from distribution_header
      join distribution_detail
        on distribution_header.dh_id = distribution_detail.dh_id
)

select *
  from final