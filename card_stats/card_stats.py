import requests
import json
import pandas as pd
from pandas.tseries.offsets import Hour, Minute, Second, Day
import time
import mysql.connector
import datetime
from urllib.parse import quote
import traceback

url = "https://suppliers-api.wildberries.ru/content/v1/analytics/nm-report/detail"

DATE_STR_FORMAT = '%Y-%m-%d'

prev_day = datetime.datetime.now() - Day(1)

START_DATE = prev_day.strftime(DATE_STR_FORMAT)
END_DATE = prev_day.strftime(DATE_STR_FORMAT)

# START_DATE = '2024-01-10'
# END_DATE = '2024-02-12'
DB_TABLE_NAME = 'card_stats'

COMPANIES = [
    {
        'company_name': 'Акс',
        'auth_token': 'Bearer eyJhbGciOiJFUzI1NiIsImtpZCI6IjIwMjMxMDI1djEiLCJ0eXAiOiJKV1QifQ.eyJlbnQiOjEsImV4cCI6MTcxODc1NjY5NSwiaWQiOiIwMTM3NjM0My05NzFlLTQ3ZWUtYWU2ZC0yNGM4NDE4ZTdjNjYiLCJpaWQiOjg2NTQ4NDcsIm9pZCI6NjcwNzYxLCJzIjo1MTAsInNhbmRib3giOmZhbHNlLCJzaWQiOiJhZmQwNWNkZC0zYTg4LTRmNTEtYThlNi1lN2I2NDVhNGM1NTYiLCJ1aWQiOjg2NTQ4NDd9.MgFRxQ1kUTT4nRLr7grLwiqr5OQ2IlPRjk8odcnmBfyUsECjjCA0mP-eR-BBnhyKESWRzJWIir99H4uPeQ6msg'
    },
    {
        'company_name': 'Онихимовский',
        'auth_token': 'Bearer eyJhbGciOiJFUzI1NiIsImtpZCI6IjIwMjMxMDI1djEiLCJ0eXAiOiJKV1QifQ.eyJlbnQiOjEsImV4cCI6MTcxODc1NjcxNywiaWQiOiJmYzZmYjIxYy1mY2UxLTRlNjYtODM2Yy00MzNjMTNiZjg5ZmYiLCJpaWQiOjg2NTQ4NDcsIm9pZCI6ODkyMDU5LCJzIjo1MTAsInNhbmRib3giOmZhbHNlLCJzaWQiOiIzYjk3YWUzYi0zOGVmLTQ1NTktYTVkNC1jN2FkZDliNWZiMTUiLCJ1aWQiOjg2NTQ4NDd9.9UOLZkSM1FSu2cBUpCxt4sxHN57EqhALJM4Ss6EPW-ElHZREudLBCNrtXCjOAHUrAUMCqekkyNYrhpXaDybxLw'
    },
    {
        'company_name': 'Чайковский',
        'auth_token': 'Bearer eyJhbGciOiJFUzI1NiIsImtpZCI6IjIwMjMxMDI1djEiLCJ0eXAiOiJKV1QifQ.eyJlbnQiOjEsImV4cCI6MTcxODc1NjczOSwiaWQiOiIxZTNhMmQ3Ny1hMTQ3LTRmNzEtOWRmMC05YTE3NWRhZmZiZjUiLCJpaWQiOjg2NTQ4NDcsIm9pZCI6OTkzODUwLCJzIjo1MTAsInNhbmRib3giOmZhbHNlLCJzaWQiOiIzMmZkNmUxNC1hNDRlLTQyMTgtYWMwMy05YjRiZTZlNGIxMTkiLCJ1aWQiOjg2NTQ4NDd9.O-gnScDeLec-pEyCDvsfExMbmZY5wo_wt80zGTS9oliCxu7ahsB7MNa9oVZ-z67s0NKVJ1PBpDAEZEBVpNZZ6g'
    },
    {
        'company_name': 'Малец',
        'auth_token': 'Bearer eyJhbGciOiJFUzI1NiIsImtpZCI6IjIwMjMxMDI1djEiLCJ0eXAiOiJKV1QifQ.eyJlbnQiOjEsImV4cCI6MTcxODc1Njc1NiwiaWQiOiI2MGY0YzU3Yy1hZDdmLTRhZGMtYjBlNC0zZTk5YWUwNTNhZDQiLCJpaWQiOjExMDM3NTAxNCwib2lkIjoxMjY1Njg0LCJzIjo1MTAsInNhbmRib3giOmZhbHNlLCJzaWQiOiJmMGNkYmQ0Yi1mOTg3LTQ4ZjYtOWYyNy02ZTg3ZjVhZGNhZjIiLCJ1aWQiOjExMDM3NTAxNH0.7868glS1Dq1Kz_XDZ9XrpQi8B9jWFY_NUrFyVdmtrmIXRwxlvGw37roz6tLKhfJ7fl1_APhyO1eXJqGiQI_-jg'
    },
    {
        'company_name': 'Чайковская',
        'auth_token': 'Bearer eyJhbGciOiJFUzI1NiIsImtpZCI6IjIwMjMxMDI1djEiLCJ0eXAiOiJKV1QifQ.eyJlbnQiOjEsImV4cCI6MTcxODc1Njc5MSwiaWQiOiI0YzJlNjZmMS0yYmUyLTRlYTUtYjk2MC02ZmY3YmU4OGU1MDciLCJpaWQiOjEwOTkxNzU5Miwib2lkIjoxMjM2MjQzLCJzIjo1MTAsInNhbmRib3giOmZhbHNlLCJzaWQiOiI1YzFlM2QzZS04MWI5LTRlNzEtYTJhZS0zYTc1ZGViMDZjNDkiLCJ1aWQiOjEwOTkxNzU5Mn0.v4SpDEkYfn3qa6irKRjUpubsxL6W28o5Zq2niFbHmUWAb0MvnRLGDhu1ji5VKPwI4csRV6ukgmWliHAHA56ibg'
    },
    {
        'company_name': 'Банишевский',
        'auth_token': 'Bearer eyJhbGciOiJFUzI1NiIsImtpZCI6IjIwMjMxMDI1djEiLCJ0eXAiOiJKV1QifQ.eyJlbnQiOjEsImV4cCI6MTcxNzgxNzQxMiwiaWQiOiIzODg5ZWRiYi0zZTY2LTQxYzYtOTg2NS03ZDc2ZWRmMTRkYTQiLCJpaWQiOjg2NTQ4NDcsIm9pZCI6MTU0MzA3LCJzIjo1MTAsInNpZCI6IjY0ODQ4YWMyLWUxNjMtNDhkNS04ODE3LWVkY2VmYjZiNWNlZSIsInVpZCI6ODY1NDg0N30.gjUmXmoHjVHck3pu-cvlMd0fe6zr5zMVn5uCpfa4yzvZVOQpLyaZq-nRQ9PQJvlphIMk6jPtL28jwYosrl4bWw'
    },
    {
        'company_name': 'ИП Пашкович',
        'auth_token': 'Bearer eyJhbGciOiJFUzI1NiIsImtpZCI6IjIwMjMxMDI1djEiLCJ0eXAiOiJKV1QifQ.eyJlbnQiOjEsImV4cCI6MTcxNzEwOTI3MSwiaWQiOiJjNTQwMDdmNC1lMjlkLTQzNGUtYTcyZC1mMmUzMTIzMjczMzUiLCJpaWQiOjQzOTk0OTE2LCJvaWQiOjI2NzA3NCwicyI6NTEwLCJzaWQiOiI5MWUxNGY0Zi05MTNmLTQzYTktOTNjZC03NjZkOTQwYTRlZTAiLCJ1aWQiOjQzOTk0OTE2fQ.Iyxn2RIAywWodXEtLbcKfXolSS-1u5KSTTeNG7PnMybyy6UHCJDlQDVkztct-hG4B7QG29sUjNJnqp1Oa7PRAw'
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


def key_alias(base_dict, alias):
    result = {}
    for key in base_dict:
        result[f'{alias}.{key}'] = base_dict[key]

    return result


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


def save_rows_to_retrafic(rows_value):
    fields_to_sync = [
        'start_date',
        'end_date',
        'nmID',
        'vendorCode',
        'selectedPeriod.begin',
        'selectedPeriod.end',
        'selectedPeriod.openCardCount',
        'selectedPeriod.addToCartCount',
        'selectedPeriod.ordersCount',
        'selectedPeriod.ordersSumRub',
        'selectedPeriod.buyoutsCount',
        'selectedPeriod.buyoutsSumRub',
        'selectedPeriod.cancelCount',
        'selectedPeriod.cancelSumRub',
        'selectedPeriod.avgPriceRub',
        'companyName',
    ]
    field_aliases = {
        'selectedPeriod.begin': 'begin',
        'selectedPeriod.end': 'end',
        'selectedPeriod.openCardCount': 'openCardCount',
        'selectedPeriod.addToCartCount': 'addToCartCount',
        'selectedPeriod.ordersCount': 'ordersCount',
        'selectedPeriod.ordersSumRub': 'ordersSumRub',
        'selectedPeriod.buyoutsCount': 'buyoutsCount',
        'selectedPeriod.buyoutsSumRub': 'buyoutsSumRub',
        'selectedPeriod.cancelCount': 'cancelCount',
        'selectedPeriod.cancelSumRub': 'cancelSumRub',
        'selectedPeriod.avgPriceRub': 'avgPriceRub',
    }
    fields_formatters = {
        # '': format_num,
    }
    for row_dict in rows_value:
        dict_to_for_save = {}
        for field in row_dict:
            if field in fields_to_sync:
                raw_value = row_dict[field]
                value = fields_formatters[field](raw_value) if field in fields_formatters else raw_value
                field_after_aliased = field_aliases[field] if field in field_aliases else field
                cleaned_field_name = field_after_aliased.replace(".", "_").lower()
                cleaned_value = value
                dict_to_for_save[cleaned_field_name] = cleaned_value

        db_save_row(dict_to_for_save)


def get_dates_range():
    result = []

    increment_start_date_value = Hour(23) + Minute(59) + Second(59)
    start_dates = pd.date_range(start=START_DATE, end=END_DATE, freq='D')
    for st in start_dates:
        end_date = st + increment_start_date_value
        result.append({
            'start': st,
            'startApi': st.strftime('%Y-%m-%d'),
            'end': end_date,
            'endApi': end_date.strftime('%Y-%m-%d'),
        })
    return result


def send_telegram_message(text_message):
    chat_id = -4020643298
    tg_token = '6236028980:AAHSNJaZQ3wFmtiviAvK26ABycfpG9vQz40'
    message = quote(f'card_stats: {text_message}')

    requests.request(
        method='GET',
        url=f'https://api.telegram.org/bot{tg_token}/sendMessage?chat_id={chat_id}&text={message}'
    )


if __name__ == '__main__':
    try:
        send_telegram_message('start')
        for range_item in get_dates_range():
            i = 0

            while len(COMPANIES) > i:
                rows = []
                company_name = COMPANIES[i]['company_name']
                payload = json.dumps({
                    "period": {
                        "begin": str(range_item['start']),
                        "end": str(range_item['end']),
                    },
                    "page": 1
                })
                headers = {
                    'Authorization': COMPANIES[i]['auth_token'],
                    'Content-Type': 'application/json'
                }

                response = None
                request_result = None
                try:
                    send_telegram_message(f'get data for {company_name} for {range_item["start"]}-{range_item["end"]}')
                    request_result = requests.request("POST", url, headers=headers, data=payload)
                    response = request_result.json()
                except Exception as err:
                    print(f'status_code {request_result.status_code}')
                    time.sleep(10)
                    continue

                flatten_data = []

                if 'data' in response and 'cards' in response['data'] and response['data']['cards'] is not None:
                    for feed in response['data']['cards']:
                        rows.append({
                            'start_date': str(range_item['start']),
                            'end_date': str(range_item['end']),
                            'companyName': company_name,
                            **feed,
                            **key_alias(feed['statistics']['selectedPeriod'], 'selectedPeriod'),
                        })

                i += 1

                send_telegram_message(
                    f'save data for {company_name} for {range_item["start"]}-{range_item["end"]}, data count: {len(rows)}')
                save_rows_to_retrafic(rows)
    except Exception as err:
        send_telegram_message(traceback.format_exc())
        raise err
    finally:
        cursor.close()
        db_connection.close()
