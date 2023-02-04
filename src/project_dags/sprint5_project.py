import logging
import pendulum
from airflow import DAG
from airflow.decorators import task
from config_const import ConfigConst
from lib import ConnectionBuilder
import json
import requests
import datetime


log = logging.getLogger(__name__)

def load_couriers_imp(pg_conn):
    with pg_conn.connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""insert into dds.dm_couriers (
                                         courier_key
                                         , courier_name
                                       )
                           select dsc.object_id as courier_key
                                  , dsc.object_value::JSON->>'name' as courier_name
                           from stg.delivery_system_couriers dsc
                           on conflict (courier_key) do update 
                           set courier_name = EXCLUDED.courier_name
                        """)
  
def load_deliveries_imp(pg_conn, execution_date):

    ds_date = datetime.date(execution_date.year, execution_date.month, execution_date.day)

    with pg_conn.connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""insert into dds.dm_deliveries (
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
                                         , (dsd.object_value::JSON->>'order_ts')::date as order_ts
                                  from stg.delivery_system_deliveries dsd) d
                            inner join dds.dm_orders do2 on (d.order_key = do2.order_key)
                            inner join dds.dm_couriers dc on (d.courier_key = dc.courier_key)
                            where to_char(d.order_ts, 'yyyy-mm-dd') = '""" + ds_date.strftime('%Y-%m-%d') + """'
                            on conflict (order_id, delivery_key, courier_id) do nothing
                        """)

def load_fact_sales_imp(pg_conn, execution_date):
    ds_date = datetime.date(execution_date.year, execution_date.month, execution_date.day)

    with pg_conn.connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""update dds.fct_product_sales p
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
                                             where p.order_id = dd.order_id)
                                               where p.order_id in (select o.id 
                                                                    from dds.dm_orders o
                                                                    inner join dds.dm_timestamps dt
                                                                    on (o.timestamp_id = dt.id)
                                                                    where to_char(dt.ts, 'yyyy-mm-dd') = '""" + ds_date.strftime('%Y-%m-%d') + """')
                        """)

def load_courier_ledger_imp(pg_conn, execution_date):
    ds_date = datetime.date(execution_date.year, execution_date.month, execution_date.day)

    with pg_conn.connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""insert into cdm.dm_courier_ledger (
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
                        where to_char(dt.ts, 'yyyy-mm-dd') = '""" + ds_date.strftime('%Y-%m-%d') + """'
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
                            , courier_reward_sum = excluded.courier_reward_sum
                        """)

headers = {
    "X-API-KEY": "25c27781-8fde-4b30-a22e-524044a7580f",
    "X-Nickname": "lictov",
    "X-Cohort": "8"
}

def load_couriers_api_imp(headers, pg_conn):

    loading_done = False
    offset = 0
    
    with pg_conn.connection() as conn:
            with conn.cursor() as cur:
                cur.execute('delete from stg.delivery_system_couriers');

    while (loading_done == False):    
        r = requests.get('https://d5d04q7d963eapoepsqr.apigw.yandexcloud.net/couriers?limit=50&offset=' + str(offset), headers=headers)    
        response_list = json.loads(r.content)    

        if len(response_list) < 50:
            loading_done = True

        offset = offset + 50

        with pg_conn.connection() as conn:
            with conn.cursor() as cur:
                for el in response_list:
                    cur.execute("""INSERT INTO stg.delivery_system_couriers
                                (object_id, object_value, load_ts)
                                VALUES(%(object_id)s, %(object_value)s, now());""",
                                {"object_id": el['_id']
                                 , "object_value": json.dumps(el)});

def load_deliveries_api_imp(headers, pg_conn, execution_date):

    loading_done = False
    offset = 0

    ds_date = execution_date

    from_date = datetime.date(ds_date.year, ds_date.month, ds_date.day)
    to_date = datetime.date(ds_date.year, ds_date.month, ds_date.day + 1)

    while (loading_done == False):    
        r = requests.get('https://d5d04q7d963eapoepsqr.apigw.yandexcloud.net/deliveries?limit=50&offset=' + str(offset)
                            + '&from=' + from_date.strftime('%Y-%m-%d %H:%M:%S') + '&to=' + to_date.strftime('%Y-%m-%d %H:%M:%S'), headers=headers)    
        response_list = json.loads(r.content)

        log.warning(from_date.strftime('%Y-%m-%d %H:%M:%S'))

        if len(response_list) < 50:
            loading_done = True

        offset = offset + 50

        with pg_conn.connection() as conn:
            with conn.cursor() as cur:
                for el in response_list:
                    cur.execute("""INSERT INTO stg.delivery_system_deliveries
                                (object_id, object_value, load_ts)
                                VALUES(%(object_id)s, %(object_value)s, now());""",
                                {"object_id": el['delivery_id']
                                 , "object_value": json.dumps(el)});


with DAG(
    dag_id='sprint5_project',
    schedule_interval='0/15 * * * *',
    start_date=pendulum.datetime(2022, 5, 5, tz="UTC"),
    catchup=False,
    tags=['sprint5', 'project'],
    is_paused_upon_creation=False
) as dag:
    dwh_pg_connect = ConnectionBuilder.pg_conn(ConfigConst.PG_WAREHOUSE_CONNECTION)

    @task(task_id="load_couriers_api")
    def load_couriers_api(ds=None, **kwargs):
        load_couriers_api_imp(headers, dwh_pg_connect)
        
    @task(task_id="load_deliveries_api")
    def load_deliveries_api(ds=None, **kwargs):
        load_deliveries_api_imp(headers, dwh_pg_connect, kwargs['execution_date'])

    @task(task_id="load_couriers")
    def load_couriers(ds=None, **kwargs):
        load_couriers_imp(dwh_pg_connect)
        
    @task(task_id="load_deliveries")
    def load_deliveries(ds=None, **kwargs):
        load_deliveries_imp(dwh_pg_connect, kwargs['execution_date'])
        
    @task(task_id="load_fact_sales")
    def load_fact_sales(ds=None, **kwargs):
        load_fact_sales_imp(dwh_pg_connect, kwargs['execution_date'])
        
    @task(task_id="load_courier_ledger")
    def load_courier_ledger(ds=None, **kwargs):
        load_courier_ledger_imp(dwh_pg_connect, kwargs['execution_date'])
  
    load_couriers_api = load_couriers_api() 
    load_deliveries_api = load_deliveries_api()
    dm_couriers = load_couriers()
    dm_deliveries = load_deliveries()
    fct_sales = load_fact_sales()
    dm_courier_ledger = load_courier_ledger()
    


    load_couriers_api >> load_deliveries_api >> dm_couriers >> dm_deliveries >> fct_sales >> dm_courier_ledger  
