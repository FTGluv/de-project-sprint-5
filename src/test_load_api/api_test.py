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

    while (loading_done == False):    
        r = requests.get('https://d5d04q7d963eapoepsqr.apigw.yandexcloud.net/couriers?limit=50&offset=' + str(offset), headers=headers)    
        response_list = json.loads(r.content)

        if len(response_list) < 50:
            loading_done = True

        offset = offset + 50

        for el in response_list:
            print(el['_id'])
            print(el['name'])

load_couriers(headers)
