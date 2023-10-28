import requests as rq
import pandas as pd
from pandas.tseries.offsets import Hour, Minute, Second
import time

START_DATE = '2023-10-08'
END_DATE = '2023-10-09'
BASE_URL = 'https://advert-api.wb.ru/adv/v1/'


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


def run_request(method, url, params):
    return rq.request(
        method=method,
        url=f'{BASE_URL}{url}',
        params=params,
        headers={
            'Authorization': 'Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJhY2Nlc3NJRCI6IjRhOThlOGRmLWVhMmEtNGRmOS05N2Q5LWQ3YjRjOTk4OTIyMiJ9._ks8m5FTrORXSEAOA_P1RwqNSzCOgEqGNsUzxcSCZCk'
        }
    ).json()


def key_alias(base_dict, alias):
    result = {}
    for key in base_dict:
        result[f'{alias}.{key}'] = base_dict[key]

    return result


if __name__ == '__main__':
    dates_range = get_dates_range()
    rows = []

    for range_item_idx, range_item in enumerate(dates_range):
        upd = run_request('GET', 'upd', {'from': range_item['startApi'], 'to': range_item['endApi']})
        for upd_item_idx, upd_item in enumerate(upd):
            range_idx = 0
            while len(dates_range) > range_idx:
                print(f'ri: {range_item_idx + 1} of {len(dates_range)}; upd: {upd_item_idx + 1} of {len(upd)}; riupd: {range_idx + 1} of {len(dates_range)}')
                d_range = dates_range[range_idx]
                data = run_request(
                    'GET',
                    'fullstat',
                    {
                        'id': upd_item['advertId'],
                        'begin': d_range['startApi'],
                        'end': d_range['endApi']
                    }
                )

                if 'code' in data and data['code'] == 429:
                    seconds_to_sleep = 10
                    print(f'{data["message"]}, sleep {seconds_to_sleep}s')
                    time.sleep(seconds_to_sleep)
                    continue

                if data['days'] is not None:
                    for day in data['days']:
                        for app in day['apps']:
                            for nm in app['nm']:
                                row = {
                                    'date': d_range['startApi'],
                                    **key_alias(upd_item, 'upd'),
                                    **key_alias(nm, 'day.apps.nm'),
                                    **key_alias(app, 'day.apps'),
                                    **key_alias(day, 'day'),
                                    **key_alias(data, 'main'),
                                }
                                rows.append(row)

                range_idx += 1

    df = pd.DataFrame.from_records(rows, index='date')
    df.to_excel('wb_fullstat.xlsx')
