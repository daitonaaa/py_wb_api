import requests as rq
import pandas as pd
from pandas.tseries.offsets import Hour, Minute, Second, Day
import time
from urllib.parse import quote
import datetime

DATE_STR_FORMAT = '%Y-%m-%d'

prev_day = datetime.datetime.now() - Day(1)

START_DATE = prev_day.strftime(DATE_STR_FORMAT)
END_DATE = prev_day.strftime(DATE_STR_FORMAT)
BASE_URL = 'https://advert-api.wb.ru/adv/v1/'
RETRAEFIC_WEBHOOK_SAVE = 'https://retrafic.ru/api/integrations/save/dac1ae569f1fb74bf39b3e85c2097b96'

COMPANIES = [
    {
        'name': 'Акс',
        'authToken': 'eyJhbGciOiJFUzI1NiIsImtpZCI6IjIwMjMxMDI1djEiLCJ0eXAiOiJKV1QifQ.eyJlbnQiOjEsImV4cCI6MTcxNTkwOTQ5OSwiaWQiOiI2MjY4MTRmZi00YjQ2LTQ2NTgtODBmMS1lN2E2YzkyNTBkZmUiLCJpaWQiOjg2NTQ4NDcsIm9pZCI6NjcwNzYxLCJzIjo2NCwic2lkIjoiYWZkMDVjZGQtM2E4OC00ZjUxLWE4ZTYtZTdiNjQ1YTRjNTU2IiwidWlkIjo4NjU0ODQ3fQ.zI6W2UOGMlTxNhTOj0IR2GZKCSNWhFwq5MVLznrQK6H8mwiz8hHm9iuQoHYYsBPbq5TOYg9fDjmrYEBiIyVvIg'
    },
    # {
    #     'name': 'Банишевский',
    #     'authToken': 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJhY2Nlc3NJRCI6ImIyMDI4ZWEwLWZjOTgtNDhjOS04ZTNmLTljZGZmNjk5NjMxYSJ9.lvvIr9_YlAn8leLklp-uq9I7gmQF-pP2Ar5bkP0VQos'
    # }
]


def save_rows_to_retrafic(rows_value):
    fields_to_sync = [
        'companyName',
        'date',
        'upd.updTime',
        'upd.campName',
        'upd.paymentType',
        'upd.updNum',
        'upd.updSum',
        'upd.advertId',
        'upd.advertType',
        'upd.advertStatus',
        'day.apps.nm.views',
        'day.apps.nm.clicks',
        'day.apps.nm.ctr',
        'day.apps.nm.cpc',
        'day.apps.nm.sum',
        'day.apps.nm.atbs',
        'day.apps.nm.orders',
        'day.apps.nm.cr',
        'day.apps.nm.shks',
        'day.apps.nm.sum_price',
        'day.apps.nm.name',
        'day.apps.nm.nmId',
    ]
    for row_dict in rows_value:
        query = ''
        for field in row_dict:
            if field in fields_to_sync:
                query += f'{field.replace(".", "_").lower()}={quote(str(row_dict[field]))}&'

        url_to_save = f'{RETRAEFIC_WEBHOOK_SAVE}?{query}'
        request_results = rq.request(method='GET', url=url_to_save)

        try:
            json = request_results.json()
            if 'code' in json and json['code'] >= 400:
                send_telegram_message(
                    f'save db retraefic error, code: {json["code"]}, {json["message"]}')
        except Exception as err:
            print(err)


def send_telegram_message(text_message):
    chat_id = -949609039
    tg_token = '6236028980:AAHSNJaZQ3wFmtiviAvK26ABycfpG9vQz40'
    message = quote(f'bronks: {text_message}')

    rq.request(
        method='GET',
        url=f'https://api.telegram.org/bot{tg_token}/sendMessage?chat_id={chat_id}&parse_mode=html&text={message}'
    )


def get_dates_range():
    result = []

    increment_start_date_value = Hour(23) + Minute(59) + Second(59)
    start_dates = pd.date_range(start=START_DATE, end=END_DATE, freq='D')
    for st in start_dates:
        end_date = st + increment_start_date_value
        result.append({
            'start': st,
            'startApi': st.strftime(DATE_STR_FORMAT),
            'end': end_date,
            'endApi': end_date.strftime(DATE_STR_FORMAT),
        })
    return result


def run_request(method, url, params, authToken):
    return rq.request(
        method=method,
        url=f'{BASE_URL}{url}',
        params=params,
        headers={
            'Authorization': f'Bearer {authToken}'
        }
    ).json()


def key_alias(base_dict, alias):
    result = {}
    for key in base_dict:
        result[f'{alias}.{key}'] = base_dict[key]

    return result


start_time = time.time()
dates_range = get_dates_range()
rows = []

try:
    send_telegram_message(f'start, date: {START_DATE}, end: {END_DATE}')

    for company in COMPANIES:
        upd = run_request('GET', 'upd', {'from': START_DATE, 'to': END_DATE}, company['authToken'])

        grouped_upd_by_id = {}
        for upd_i in upd:
            if upd_i['advertId'] in grouped_upd_by_id:
                grouped_upd_by_id[upd_i['advertId']]['updSum'] += upd_i['updSum']
                continue

            grouped_upd_by_id[upd_i['advertId']] = upd_i

        for upd_item_idx, advertId in enumerate(grouped_upd_by_id):
            upd_item = grouped_upd_by_id[advertId]
            range_idx = 0
            while len(dates_range) > range_idx:
                print(f'upd: {upd_item_idx + 1} of {len(upd)}; riupd: {range_idx + 1} of {len(dates_range)}')
                d_range = dates_range[range_idx]
                data = run_request(
                    'GET',
                    'fullstat',
                    {
                        'id': upd_item['advertId'],
                        'begin': d_range['startApi'],
                        'end': d_range['endApi']
                    },
                    company['authToken']
                )

                if 'code' in data and data['code'] == 429:
                    seconds_to_sleep = 10
                    print(f'{data["message"]}, sleep {seconds_to_sleep}s')
                    time.sleep(seconds_to_sleep)
                    continue

                if 'days' in data and data['days'] is not None:
                    for day in data['days']:
                        for app in day['apps']:
                            for nm in app['nm']:
                                row = {
                                    'companyName': company['name'],
                                    'date': d_range['startApi'],
                                    **key_alias(upd_item, 'upd'),
                                    **key_alias(nm, 'day.apps.nm'),
                                    **key_alias(app, 'day.apps'),
                                    **key_alias(day, 'day'),
                                    **key_alias(data, 'main'),
                                }
                                rows.append(row)

                range_idx += 1
except Exception as err:
    send_telegram_message(f'error: {err}')

if len(rows) > 0:
    send_telegram_message('rows count > 0, start save to db')
    save_rows_to_retrafic(rows)

send_telegram_message(f'end, total rows man {len(rows)}, \nruntime seconds: {time.time() - start_time}')
