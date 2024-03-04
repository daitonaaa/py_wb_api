import requests as rq
import pandas as pd
from pandas.tseries.offsets import Hour, Minute, Second, Day
import time
from urllib.parse import quote
import datetime
import traceback
import mysql.connector

DATE_STR_FORMAT = '%Y-%m-%d'

prev_day = datetime.datetime.now() - Day(1)

START_DATE = prev_day.strftime(DATE_STR_FORMAT)
END_DATE = prev_day.strftime(DATE_STR_FORMAT)
BASE_URL = 'https://advert-api.wb.ru/adv/v1/'
DB_TABLE_NAME = 'bronks'

## -------- TEST DATA
# START_DATE = '2024-02-18'
# END_DATE = '2024-02-18'

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
    },
    {
        'name': 'Малец',
        'authToken': 'eyJhbGciOiJFUzI1NiIsImtpZCI6IjIwMjMxMDI1djEiLCJ0eXAiOiJKV1QifQ.eyJlbnQiOjEsImV4cCI6MTcxODIyNDIyMCwiaWQiOiIwNGNhN2VjMS1jOGI4LTQ1OTUtODc4Ny1kMDRiNDMyODY1M2IiLCJpaWQiOjExMDM3NTAxNCwib2lkIjoxMjY1Njg0LCJzIjo1MTAsInNhbmRib3giOmZhbHNlLCJzaWQiOiJmMGNkYmQ0Yi1mOTg3LTQ4ZjYtOWYyNy02ZTg3ZjVhZGNhZjIiLCJ1aWQiOjExMDM3NTAxNH0.lF-yRiL1eal2FCjvrAHQiwMP-iZqRMhw6pHXD4liGJxZTDksbPg4O9tFfZW7Npkz71qrylzFxyFdvg1oum_e8w'
    },
    {
        'name': 'Чайковская',
        'authToken': 'eyJhbGciOiJFUzI1NiIsImtpZCI6IjIwMjMxMDI1djEiLCJ0eXAiOiJKV1QifQ.eyJlbnQiOjEsImV4cCI6MTcxODIyNDM2OSwiaWQiOiJhNmQ0NDllOC01NTNmLTRhMmYtYjUzZC0zZmYyNzYyMTI0ODMiLCJpaWQiOjEwOTkxNzU5Miwib2lkIjoxMjM2MjQzLCJzIjo1MTAsInNhbmRib3giOmZhbHNlLCJzaWQiOiI1YzFlM2QzZS04MWI5LTRlNzEtYTJhZS0zYTc1ZGViMDZjNDkiLCJ1aWQiOjEwOTkxNzU5Mn0.Thk3SuT5WVyufRo7PQ91vaWWdDz8UmigQONptWiMu4-mLadYBW5ikopadW88riW9ieYZDhIDd_ACOgCGUfYr7A'
    },
    {
        'name': 'Пашкович',
        'authToken': 'eyJhbGciOiJFUzI1NiIsImtpZCI6IjIwMjMxMDI1djEiLCJ0eXAiOiJKV1QifQ.eyJlbnQiOjEsImV4cCI6MTcxNzEwOTI3MSwiaWQiOiJjNTQwMDdmNC1lMjlkLTQzNGUtYTcyZC1mMmUzMTIzMjczMzUiLCJpaWQiOjQzOTk0OTE2LCJvaWQiOjI2NzA3NCwicyI6NTEwLCJzaWQiOiI5MWUxNGY0Zi05MTNmLTQzYTktOTNjZC03NjZkOTQwYTRlZTAiLCJ1aWQiOjQzOTk0OTE2fQ.Iyxn2RIAywWodXEtLbcKfXolSS-1u5KSTTeNG7PnMybyy6UHCJDlQDVkztct-hG4B7QG29sUjNJnqp1Oa7PRAw'
    }
]

db_connection = mysql.connector.connect(
    host="db.retrafic.ru",
    user="user344",
    password="1d9dce46940f625466943eacc499718f",
    database="user344",
    port=3306
)

cursor = db_connection.cursor()


def db_save_row(row):
    fields_arr = []
    for field in row:
        fields_arr.append(field)

    sql_query = f"INSERT INTO {DB_TABLE_NAME} ({', '.join(fields_arr)}) VALUES ({', '.join(['%s' for item in fields_arr])})"

    values = ()
    for field in row:
        values = values + (row[field],)

    cursor.execute(sql_query, values)
    db_connection.commit()


def raise_if_api_err(api_result):
    if 'is_error' in api_result:
        raise Exception(f'{api_result["status_code"]}:{api_result["reason"]}')
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
        dict_to_for_save = {}
        for field in row_dict:
            if field in fields_to_sync:
                raw_value = row_dict[field]
                value = fields_formatters[field](raw_value) if field in fields_formatters else raw_value
                cleaned_field = field.replace(".", "_").lower()
                cleaned_value = value
                dict_to_for_save[cleaned_field] = cleaned_value

        db_save_row(dict_to_for_save)


def send_telegram_message(text_message):
    chat_id = -4020643298
    tg_token = '6236028980:AAHSNJaZQ3wFmtiviAvK26ABycfpG9vQz40'
    message = quote(f'bronks: {text_message}')

    try:
        rq.request(
            method='GET',
            url=f'https://api.telegram.org/bot{tg_token}/sendMessage?chat_id={chat_id}&text={message}'
        )
    except Exception as err:
        print(err)


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
    request_result = rq.request(
        method=method,
        url=f'{BASE_URL}{url}',
        params=params,
        headers={
            'Authorization': f'Bearer {authToken}'
        }
    )

    try:
        return request_result.json()
    except:
        return {'is_error': True, 'reason': request_result.reason, 'status_code': request_result.status_code}


def key_alias(base_dict, alias):
    result = {}
    for key in base_dict:
        result[f'{alias}.{key}'] = base_dict[key]

    return result


if __name__ == '__main__':
    start_time = time.time()
    dates_range = get_dates_range()

    send_telegram_message(f'********* ********* \nSTART NEW PROCESS for dates {START_DATE} - {END_DATE}')

    try:
        for d_range in dates_range:
            results = {}

            for company in COMPANIES:
                rows = []

                try:
                    send_telegram_message(f'company start 🚀: {company["name"]} date: {d_range["startApi"]}')
                    upd = run_request('GET', 'upd', {'from': d_range['startApi'], 'to': d_range['endApi']},
                                      company['authToken'])
                    raise_if_api_err(upd)

                    grouped_upd_by_id = {}
                    for upd_i in upd:
                        if upd_i['advertId'] in grouped_upd_by_id:
                            grouped_upd_by_id[upd_i['advertId']]['updSum'] += upd_i['updSum']
                            continue

                        grouped_upd_by_id[upd_i['advertId']] = upd_i

                    upd_items = list(grouped_upd_by_id.values())
                    upd_item_idx = 0
                    while len(upd_items) > upd_item_idx:
                        upd_item = upd_items[upd_item_idx]

                        print(f'upd: {upd_item_idx + 1} of {len(upd)}')
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

                        if 'code' in data and data['code'] == 429 or 'is_error' in data:
                            seconds_to_sleep = 10
                            print(f'sleep {seconds_to_sleep}s')
                            time.sleep(seconds_to_sleep)
                            continue

                        # raise_if_api_err(data)

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

                        upd_item_idx += 1
                except Exception as err:
                    send_telegram_message(f'company ❌ ERR: {company["name"]}, date: {d_range["startApi"]}, {err}')
                    send_telegram_message(traceback.format_exc())
                    rows = []
                    continue

                results[company['name']] = rows
                send_telegram_message(f'data success received  ✅: {company["name"]}, date: {d_range["startApi"]}')

            for company_name in results:
                company_rows = results[company_name]
                save_rows_to_retrafic(company_rows)
                send_telegram_message(f'saved data for {company_name}, total: {len(company_rows)} rows')

        total_seconds_formatted = "{:10.2f}".format(time.time() - start_time)
        send_telegram_message(
            f'END PROCESS, date ranges total: {len(dates_range)} \nruntime: {total_seconds_formatted} sec.')
    except Exception as err:
        raise err
    finally:
        cursor.close()
        db_connection.close()
