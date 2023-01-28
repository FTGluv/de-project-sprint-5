drop table if exists stg.delivery_system_deliveries;

create table stg.delivery_system_deliveries (
  id serial primary key
  , object_id varchar
  , object_value text
  , load_ts timestamp
);