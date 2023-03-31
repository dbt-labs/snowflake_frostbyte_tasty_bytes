-- Make this an incremental

SELECT * 
  FROM {{ ref('daily_order_profit') }}