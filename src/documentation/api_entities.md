id | autogenerated
courier_id | dds.dm_deliveries (new) | GET / deliveries
courier_name | dds.dm_couriers (new) | GET /couriers
settlement_year | dds.dm_timestamps (existing)
settlement_month | dds.dm_timestamps (existing)
orders_count | fct_product_sales (existing)
orders_total_sum | fct_product_sales (existing)
rate_avg | fct_product_sales (existing, extended) | GET / deliveries
order_processing_fee | calculated
courier_order_sum | calculated
courier_tips_sum | fct_product_sales (existing, extended) | GET / deliveries
courier_reward_sum | calculated