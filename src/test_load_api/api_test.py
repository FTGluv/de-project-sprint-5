import datetime
import requests
import json

headers = {
    "X-API-KEY": "25c27781-8fde-4b30-a22e-524044a7580f",
    "X-Nickname": "lictov",
    "X-Cohort": "8"
}


def load_couriers(headers):

    loading_done = False
    offset = 0

    ds_date = datetime.date(2023, 1, 11)

    from_date = datetime.date(ds_date.year, ds_date.month, ds_date.day)
    to_date = datetime.date(ds_date.year, ds_date.month, ds_date.day + 1)
    

    while (loading_done == False):    
        r = requests.get('https://d5d04q7d963eapoepsqr.apigw.yandexcloud.net/deliveries?limit=50&offset=' + str(offset)
                            + '&from=' + from_date.strftime('%Y-%m-%d %H:%M:%S') + '&to=' + to_date.strftime('%Y-%m-%d %H:%M:%S'), headers=headers)    
        response_list = json.loads(r.content)

        if len(response_list) < 50:
            loading_done = True

        offset = offset + 50

        for el in response_list:
            print(el['delivery_id'])

load_couriers(headers)
