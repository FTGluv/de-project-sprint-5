delete from stg.delivery_system_deliveries;

insert into stg.delivery_system_deliveries (
  object_id
  , object_value
  , load_ts)
values (
  '69fivpa596a6nu1yx8159vh'
  , '{
        "order_id": "63aa7e3a62f6f263d077abcb",
        "order_ts": "2022-12-27 05:10:18.706000",
        "delivery_id": "69fivpa596a6nu1yx8159vh",
        "courier_id": "ao63ozw0hr11eft9lix19s2",
        "address": "Ул. Металлургов, 13, кв. 355",
        "delivery_ts": "2022-12-27 05:44:23.765000",
        "rate": 5,
        "sum": 1036,
        "tip_sum": 51
    }'
  , now()
);

insert into stg.delivery_system_deliveries (
  object_id
  , object_value
  , load_ts)
values (
  'hqpc8jilcqpb81r9ysbgink'
  , '{
        "order_id": "63aa82ebd8c5e3d3e8145af5",
        "order_ts": "2022-12-27 05:30:19.629000",
        "delivery_id": "hqpc8jilcqpb81r9ysbgink",
        "courier_id": "cdza6as22kgop2v7ios1m07",
        "address": "Ул. Садовая, 7, кв. 448",
        "delivery_ts": "2022-12-27 06:35:23.765000",
        "rate": 5,
        "sum": 2376,
        "tip_sum": 237
    }'
  , now()
);

insert into stg.delivery_system_deliveries (
  object_id
  , object_value
  , load_ts)
values (
  'sehgbe0jw0utkutggpxw6yu'
  , '{
        "order_id": "63aa81bed57f940b3e717248",
        "order_ts": "2022-12-27 05:25:18.465000",
        "delivery_id": "sehgbe0jw0utkutggpxw6yu",
        "courier_id": "b8q5vmdtp1b2dcvnobugfrw",
        "address": "Ул. Зеленая, 4, кв. 476",
        "delivery_ts": "2022-12-27 06:14:23.774000",
        "rate": 5,
        "sum": 3789,
        "tip_sum": 378
    }'
  , now()
);