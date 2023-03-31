with purchase_order_header as (

    select * from {{ ref('stg_purchase_order_header') }}

),
purchase_order_detail as (

    select * from {{ ref('stg_purchase_order_detail') }}
    
),

final as (

    select purchase_order_header.warehouse_id,
           purchase_order_header.po_id, 
           purchase_order_header.vendor_id,
           purchase_order_header.po_date,
           purchase_order_header.po_total,
           purchase_order_header.shipment_date,
           purchase_order_header.delivery_date,
           purchase_order_detail.line_item_id,
           purchase_order_detail.item_id,
           purchase_order_detail.quantity, 
           purchase_order_detail.manufacturing_date,
           purchase_order_detail.expiration_date,
      from purchase_order_header
      join purchase_order_detail
        on purchase_order_header.po_id = purchase_order_detail.po_id
)

select *
  from final