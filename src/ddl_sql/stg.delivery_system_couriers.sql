drop table if exists stg.delivery_system_couriers;

create table stg.delivery_system_couriers (
  id serial primary key
  , object_id varchar
  , object_value text
  , load_ts timestamp
);