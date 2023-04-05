with distribution_header as (

    select * from {{ ref('stg_distribution_header') }}

),
distribution_detail as (

    select * from {{ ref('stg_distribution_detail') }}
    
),

final as (

    select distribution_header.distribution_date,
           distribution_header.truck_id,
           distribution_header.warehouse_id, 
           distribution_detail.item_id,
           distribution_detail.expiration_date,
           distribution_detail.po_id,
           distribution_detail.quantity
      from distribution_header
      join distribution_detail
        on distribution_header.dh_id = distribution_detail.dh_id
)

select *
  from final