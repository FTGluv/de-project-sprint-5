drop table if exists dds.dm_couriers;

create table dds.dm_couriers (
  id serial primary key
  , courier_key varchar
  , courier_name varchar
);