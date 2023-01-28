drop table if exists cdm.dm_courier_ledger;

create table cdm.dm_courier_ledger (
  id serial primary key
  , courier_id integer
  , courier_name varchar
  , settlement_year integer
  , settlement_month integer
  , orders_count integer
  , orders_total_sum numeric(14,2)
  , rate_avg numeric(6,2)
  , order_processing_fee numeric(14,2)
  , courier_order_sum numeric(14,2)
  , courier_tips_sum numeric(14,2)
  , courier_reward_sum numeric(14,2)
  , unique (courier_id, courier_name, settlement_year, settlement_month)
);