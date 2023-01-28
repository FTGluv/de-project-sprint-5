insert into cdm.dm_courier_ledger (
  courier_id
  , courier_name 
  , settlement_year
  , settlement_month 
  , orders_count 
  , orders_total_sum 
  , rate_avg
  , order_processing_fee 
  , courier_order_sum 
  , courier_tips_sum 
  , courier_reward_sum) 
select dd.courier_id 
       , dc.courier_name 
       , dt."year" as settlement_year
       , dt."month" as settlement_month
       , sum(do2.orders_count) as orders_count
       , sum(do2.orders_total_sum) as orders_total_sum
       , avg(do2.rate) as rate_avg
       , sum(do2.orders_total_sum) * 0.25 as order_processing_fee
       , case when avg(do2.rate) < 4 then
                greatest(sum(do2.orders_total_sum) * 0.05, 100)
              when avg(do2.rate) < 4.5 then 
                greatest(sum(do2.orders_total_sum) * 0.07, 150)
              when avg(do2.rate) < 4.9 then 
                greatest(sum(do2.orders_total_sum) * 0.08, 175)
              else
                greatest(sum(do2.orders_total_sum) * 0.10, 200)
         end as courier_order_sum
       , sum(do2.tip_sum) as courier_tips_sum
       , case when avg(do2.rate) < 4 then
                greatest(sum(do2.orders_total_sum) * 0.05, 100)
              when avg(do2.rate) < 4.5 then 
                greatest(sum(do2.orders_total_sum) * 0.07, 150)
              when avg(do2.rate) < 4.9 then 
                greatest(sum(do2.orders_total_sum) * 0.08, 175)
              else
                greatest(sum(do2.orders_total_sum) * 0.10, 200)
         end + sum(do2.tip_sum) * 0.95 as courier_reward_sum
from dds.dm_deliveries dd 
inner join dds.dm_couriers dc on (dd.courier_id = dc.id)
inner join (select o.id as id 
                   , o.timestamp_id as timestamp_id
                   , sum(fps.count) as orders_count
                   , sum(fps.total_sum) as orders_total_sum
                   , max(fps.rate) as rate
                   , max(fps.tip_sum) as tip_sum
            from dds.dm_orders o
            inner join dds.fct_product_sales fps on (o.id = fps.order_id)
            group by o.id
                     , o.timestamp_id) do2
on (dd.order_id = do2.id)
inner join dds.dm_timestamps dt on (do2.timestamp_id = dt.id)
group by dd.courier_id 
         , dc.courier_name 
         , dt."year" 
         , dt."month"
on conflict (courier_id, courier_name, settlement_year, settlement_month) do update 
set orders_count = excluded.orders_count
    , orders_total_sum = excluded.orders_total_sum 
    , rate_avg = excluded.rate_avg
    , order_processing_fee = excluded.order_processing_fee 
    , courier_order_sum = excluded.courier_order_sum 
    , courier_tips_sum = excluded.courier_tips_sum 
    , courier_reward_sum = excluded.courier_reward_sum;