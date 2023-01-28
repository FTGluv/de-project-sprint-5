update dds.fct_product_sales p
set rate = (select d.rate 
            from (select dsd.object_id as delivery_key
                         , (dsd.object_value::JSON->>'rate')::numeric as rate
                         , (dsd.object_value::JSON->>'tip_sum')::numeric as tip_sum
                  from stg.delivery_system_deliveries dsd) d
            inner join dds.dm_deliveries dd on (d.delivery_key = dd.delivery_key)
            where p.order_id = dd.order_id)
    , tip_sum = (select d.tip_sum 
                 from (select dsd.object_id as delivery_key
                              , (dsd.object_value::JSON->>'rate')::numeric as rate
                              , (dsd.object_value::JSON->>'tip_sum')::numeric as tip_sum
                       from stg.delivery_system_deliveries dsd) d
                       inner join dds.dm_deliveries dd on (d.delivery_key = dd.delivery_key)
                 where p.order_id = dd.order_id);