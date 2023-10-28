import requests as rq
import pandas as pd
from pandas.tseries.offsets import Hour, Minute, Second
import time

# 1) Сровнять вложенные структуры данных с плоским списком
# Делаем рендер дата фрейма на основании возвращаемых данных
# только в возращаемом типе большая глубина (массивы, обьект) и необходимо
# сровнять все данные в плоский список, добираемся до корня всех вложенностей
# и поднимается наверх. Самый последний потомок это одна строка в таблице и плюс данные всех потомков

# 2) Получение данных и джойн их с полным списком
# Нижний метод который закоментирован отдает нам список всех компаний после получения
# нужно по каждому элементу запросить дополнительно fullstat метод для получения деталки
# и загнать все в дф. fullstat должен работать на тот же интервал дат что и этот метод
# - учесть что у апи вб есть ограничения и надо ожидать их, может просто ловить ошибку и падать в сон на минуту
# - учесть что интервал дат который передается может раскладываться на частотность по дням


START_DATE = '2023-10-08'
END_DATE = '2023-10-10'
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

    for range_item in dates_range:
        upd = run_request('GET', 'upd', {'from': range_item['startApi'], 'to': range_item['endApi']})
        for upd_item in upd:
            range_idx = 0
            while len(dates_range) > range_idx:
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
                    print(f'{data["message"]}, sleep 10s')
                    time.sleep(10)
                    continue

                range_idx += 1

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

    df = pd.DataFrame.from_records(rows, index='date')
    df.to_excel('wb_fullstat.xlsx')
