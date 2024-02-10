import requests
import json
import pandas as pd
from pandas.tseries.offsets import Hour, Minute, Second
import time
import mysql.connector

url = "https://suppliers-api.wildberries.ru/content/v1/analytics/nm-report/detail"

START_DATE = '2023-09-01'
END_DATE = '2023-09-05'
DB_TABLE_NAME = 'card_stats'

COMPANIES = [
    {
        'company_name': 'Малец',
        'auth_token': 'Bearer eyJhbGciOiJFUzI1NiIsImtpZCI6IjIwMjMxMDI1djEiLCJ0eXAiOiJKV1QifQ.eyJlbnQiOjEsImV4cCI6MTcxODc1NjcxNywiaWQiOiJmYzZmYjIxYy1mY2UxLTRlNjYtODM2Yy00MzNjMTNiZjg5ZmYiLCJpaWQiOjg2NTQ4NDcsIm9pZCI6ODkyMDU5LCJzIjo1MTAsInNhbmRib3giOmZhbHNlLCJzaWQiOiIzYjk3YWUzYi0zOGVmLTQ1NTktYTVkNC1jN2FkZDliNWZiMTUiLCJ1aWQiOjg2NTQ4NDd9.9UOLZkSM1FSu2cBUpCxt4sxHN57EqhALJM4Ss6EPW-ElHZREudLBCNrtXCjOAHUrAUMCqekkyNYrhpXaDybxLw'
    }
]

db_connection = mysql.connector.connect(
    host="db.retrafic.ru",
    user="user344",
    password="979ce803d58bdd90f13826bb155fda57",
    database="user344"
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
        'selectedPeriod.avgOrdersCountPerDay',
        'selectedPeriod.avgPriceRub',
        'companyName',
    ]
    field_aliases = {
        # 'id': 'reportid'
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


if __name__ == '__main__':
    try:
        rows = []

        dates_range = get_dates_range()
        i = 0
        company_name = 'Малец'

        while len(dates_range) > i:
            range_item = dates_range[i]
            payload = json.dumps({
                "period": {
                    "begin": str(range_item['start']),
                    "end": str(range_item['end']),
                },
                "page": 1
            })
            headers = {
                'Authorization': 'Bearer eyJhbGciOiJFUzI1NiIsImtpZCI6IjIwMjMxMDI1djEiLCJ0eXAiOiJKV1QifQ.eyJlbnQiOjEsImV4cCI6MTcxODc1NjcxNywiaWQiOiJmYzZmYjIxYy1mY2UxLTRlNjYtODM2Yy00MzNjMTNiZjg5ZmYiLCJpaWQiOjg2NTQ4NDcsIm9pZCI6ODkyMDU5LCJzIjo1MTAsInNhbmRib3giOmZhbHNlLCJzaWQiOiIzYjk3YWUzYi0zOGVmLTQ1NTktYTVkNC1jN2FkZDliNWZiMTUiLCJ1aWQiOjg2NTQ4NDd9.9UOLZkSM1FSu2cBUpCxt4sxHN57EqhALJM4Ss6EPW-ElHZREudLBCNrtXCjOAHUrAUMCqekkyNYrhpXaDybxLw',
                'Content-Type': 'application/json'
            }

            response = None
            request_result = None
            try:
                print(f'{i}/{len(dates_range)}')
                request_result = requests.request("POST", url, headers=headers, data=payload)
                response = request_result.json()
            except Exception as err:
                print(f'status_code {request_result.status_code}')
                print(err)
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

        save_rows_to_retrafic(rows)

    finally:
        cursor.close()
        db_connection.close()
