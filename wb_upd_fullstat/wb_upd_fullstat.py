import pandas
import requests as rq
import pandas as pd
from pandas.tseries.offsets import Hour, Minute, Second, Day
import time
from urllib.parse import quote
import datetime

DATE_STR_FORMAT = '%Y-%m-%d'

prev_day = datetime.datetime.now() - Day(1)

# START_DATE = prev_day.strftime(DATE_STR_FORMAT)
# END_DATE = prev_day.strftime(DATE_STR_FORMAT)
START_DATE = '2023-12-06'
END_DATE = '2023-12-08'
BASE_URL = 'https://advert-api.wb.ru/adv/v1/'
# RETRAEFIC_WEBHOOK_SAVE = 'https://retrafic.ru/api/integrations/save/dac1ae569f1fb74bf39b3e85c2097b96'
RETRAEFIC_WEBHOOK_SAVE = 'https://retrafic.ru/api/integrations/save/feee29adc3d3d63215705a76e7cc7d92'

COMPANIES = [
    {
        'name': 'Акс',
        'authToken': 'eyJhbGciOiJFUzI1NiIsImtpZCI6IjIwMjMxMDI1djEiLCJ0eXAiOiJKV1QifQ.eyJlbnQiOjEsImV4cCI6MTcxNTkwOTQ5OSwiaWQiOiI2MjY4MTRmZi00YjQ2LTQ2NTgtODBmMS1lN2E2YzkyNTBkZmUiLCJpaWQiOjg2NTQ4NDcsIm9pZCI6NjcwNzYxLCJzIjo2NCwic2lkIjoiYWZkMDVjZGQtM2E4OC00ZjUxLWE4ZTYtZTdiNjQ1YTRjNTU2IiwidWlkIjo4NjU0ODQ3fQ.zI6W2UOGMlTxNhTOj0IR2GZKCSNWhFwq5MVLznrQK6H8mwiz8hHm9iuQoHYYsBPbq5TOYg9fDjmrYEBiIyVvIg'
    },
    {
        'name': 'Банишевский',
        'authToken': 'eyJhbGciOiJFUzI1NiIsImtpZCI6IjIwMjMxMDI1djEiLCJ0eXAiOiJKV1QifQ.eyJlbnQiOjEsImV4cCI6MTcxNzgxNzQxMiwiaWQiOiIzODg5ZWRiYi0zZTY2LTQxYzYtOTg2NS03ZDc2ZWRmMTRkYTQiLCJpaWQiOjg2NTQ4NDcsIm9pZCI6MTU0MzA3LCJzIjo1MTAsInNpZCI6IjY0ODQ4YWMyLWUxNjMtNDhkNS04ODE3LWVkY2VmYjZiNWNlZSIsInVpZCI6ODY1NDg0N30.gjUmXmoHjVHck3pu-cvlMd0fe6zr5zMVn5uCpfa4yzvZVOQpLyaZq-nRQ9PQJvlphIMk6jPtL28jwYosrl4bWw'
    },
    {
        'name': 'Онихимовский',
        'authToken': 'eyJhbGciOiJFUzI1NiIsImtpZCI6IjIwMjMxMDI1djEiLCJ0eXAiOiJKV1QifQ.eyJlbnQiOjEsImV4cCI6MTcxNzgxNzQzOCwiaWQiOiI0Zjc2NjAyNi1lZDEzLTQ2NjQtOGExOC02Y2E1YzgwNGIyZDIiLCJpaWQiOjg2NTQ4NDcsIm9pZCI6ODkyMDU5LCJzIjo1MTAsInNpZCI6IjNiOTdhZTNiLTM4ZWYtNDU1OS1hNWQ0LWM3YWRkOWI1ZmIxNSIsInVpZCI6ODY1NDg0N30.JUYg8ZJMBEMH282NI6nb6pqA8SytCZqsxDDHsuUoi3Dbt-OjSvkBF5Y5dYsCxwm_wEdoAOoaNifKBRzFo7u5gw'
    },
    {
        'name': 'Чайковский',
        'authToken': 'eyJhbGciOiJFUzI1NiIsImtpZCI6IjIwMjMxMDI1djEiLCJ0eXAiOiJKV1QifQ.eyJlbnQiOjEsImV4cCI6MTcxNzgxNzQ2NiwiaWQiOiI5ZjM3NDFjMC0xZThlLTRlMmYtOWNlMC1iYjBjNzY5NWE5ZTciLCJpaWQiOjg2NTQ4NDcsIm9pZCI6OTkzODUwLCJzIjo1MTAsInNpZCI6IjMyZmQ2ZTE0LWE0NGUtNDIxOC1hYzAzLTliNGJlNmU0YjExOSIsInVpZCI6ODY1NDg0N30.APt3UFIYvSJW5pkhW_hnqlRSB81z19_3RRDhA9T57Ppg1VSZFy0sXnTtSFs4mGcCdOmBlWbKOCsTtjRgRWGqmA'
    }
]


def raise_if_api_err(api_result):
    if 'code' in api_result and api_result['code'] >= 400:
        raise Exception(api_result["message"])


def format_num(raw):
    try:
        formatted_value = "{:10.2f}".format(raw)
        return formatted_value
    except:
        return raw


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
    fields_formatters = {
        'day.apps.nm.views': format_num,
        'day.apps.nm.clicks': format_num,
        'day.apps.nm.ctr': format_num,
        'day.apps.nm.cpc': format_num,
        'day.apps.nm.sum': format_num,
        'day.apps.nm.atbs': format_num,
        'day.apps.nm.orders': format_num,
        'day.apps.nm.cr': format_num,
        'day.apps.nm.shks': format_num,
        'day.apps.nm.sum_price': format_num,
    }
    for row_dict in rows_value:
        query = ''
        for field in row_dict:
            if field in fields_to_sync:
                raw_value = row_dict[field]
                value = fields_formatters[field](raw_value) if field in fields_formatters else raw_value
                query += f'{field.replace(".", "_").lower()}={quote(str(value))}&'

        url_to_save = f'{RETRAEFIC_WEBHOOK_SAVE}?{query}'
        request_results = rq.request(method='GET', url=url_to_save)
        raise_if_api_err(request_results.json())


def send_telegram_message(text_message):
    chat_id = -4020643298
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
results = {}

send_telegram_message(f'********* ********* \nstart new process for dates {START_DATE} - {END_DATE}')

for company in COMPANIES:
    rows = []

    try:
        send_telegram_message(f'company start 🚀: {company["name"]}')
        upd = run_request('GET', 'upd', {'from': START_DATE, 'to': END_DATE}, company['authToken'])
        raise_if_api_err(upd)

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

                raise_if_api_err(data)

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
        send_telegram_message(f'company ❌ ERR: {company["name"]}, {err}')
        rows = []
        continue

    results[company['name']] = rows
    send_telegram_message(f'data success received  ✅: {company["name"]}')

for company_name in results:
    company_rows = results[company_name]
    save_rows_to_retrafic(company_rows)
    send_telegram_message(f'saved data for {company_name}, total: {len(company_rows)} rows')

total_seconds_formatted = "{:10.2f}".format(time.time() - start_time)
send_telegram_message(f'end, total rows man {len(results)}, \nruntime: {total_seconds_formatted} sec.')

