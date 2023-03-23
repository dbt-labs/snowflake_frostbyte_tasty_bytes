CREATE OR REPLACE VIEW frostbyte_tasty_bytes.analytics.daily_order_profit_v 
COMMENT = 'Daily Order Level Sales, COGS, Gross Profit and Sales Margin Percentages'
    AS
SELECT * FROM frostbyte_tasty_bytes.harmonized.daily_order_profit_dt;