drop table if exists dds.dm_deliveries;

create table dds.dm_deliveries (
  id serial primary key
  , order_id integer
  , delivery_key varchar
  , courier_id integer
  , unique (order_id, delivery_key, courier_id)
);

alter table dds.dm_deliveries add constraint dm_deliveries_order_fk
foreign key (order_id) references dds.dm_orders(id);

alter table dds.dm_deliveries add constraint dm_deliveries_courier_fk
foreign key (courier_id) references dds.dm_couriers(id);


