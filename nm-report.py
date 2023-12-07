import requests
import json
import pandas as pd
from pandas.tseries.offsets import Hour, Minute, Second

url = "https://suppliers-api.wildberries.ru/content/v1/analytics/nm-report/detail"

START_DATE = '2023-10-29'
END_DATE = '2023-10-30'


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
    rows = []

    for range_item in get_dates_range():
        payload = json.dumps({
            "period": {
                "begin": str(range_item['start']),
                "end":  str(range_item['end']),
            },
            "page": 1
        })
        headers = {
            'Authorization': 'Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJhY2Nlc3NJRCI6IjVhYmM5YWM1LWIyMDgtNDYyOS05NGQzLTcwYWViNjNkNjM5ZSJ9.JdE5NE96cGDx618Na4kjczyNEMXQv4PFAIA9eelhhbo',
            'Content-Type': 'application/json'
        }
        response = requests.request("POST", url, headers=headers, data=payload).json()

        # print(response)

        df = pd.DataFrame.from_dict(response)

        flatten_data = []

        if 'data' in response and 'cards' in response['data'] and response['data']['cards'] is not None:
            for feed in response['data']['cards']:
                rows.append({
                    'start_date': str(range_item['start']),
                    'end_date': str(range_item['end']),
                    **feed,
                    **feed['object'],
                    **feed,
                    **feed['statistics']['selectedPeriod'],
                    #      **feed['statistics']['previousPeriod'],
                    **feed['statistics']['periodComparison']

                })

    df = pd.DataFrame.from_records(rows, index="start_date")

    df.to_excel('text.xlsx')
