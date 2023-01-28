insert into dds.dm_couriers (
  courier_key
  , courier_name
)
select dsc.object_id as courier_key
       , dsc.object_value::JSON->>'name' as courier_name
from stg.delivery_system_couriers dsc
on conflict (courier_key) do update 
set courier_name = EXCLUDED.courier_name;