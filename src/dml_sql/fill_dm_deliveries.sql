insert into dds.dm_deliveries (
  order_id
  , delivery_key
  , courier_id
)
select do2.id as order_id
       , d.delivery_key as delivery_key
       , dc.id as courier_id
from (select dsd.object_value::JSON->>'order_id' as order_key
             , dsd.object_id as delivery_key
             , dsd.object_value::JSON->>'courier_id' as courier_key
      from stg.delivery_system_deliveries dsd) d
inner join dds.dm_orders do2 on (d.order_key = do2.order_key)
inner join dds.dm_couriers dc on (d.courier_key = dc.courier_key)
on conflict (order_id, delivery_key, courier_id) do nothing;