import requests as rq
import pandas as pd

PRODUCT_IDS = [
    51903406,
    19663908
]
START_DATE = '2023-08-28'
END_DATE = '2023-09-10'

# don't touch this
WB_API_TOKEN = '64d4cb0f3e13e5.048170039dcc9d3bc2b4f5ef8ea45510cb33335c'
WB_API_BASE_URL = 'https://mpstats.io/api/wb/'


def reduce(function, iterable, initializer=None):
    it = iter(iterable)
    if initializer is None:
        value = next(it)
    else:
        value = initializer
    for element in it:
        value = function(value, element)
    return value


def run_request(method, url, params):
    return rq.request(
        method=method,
        url=f'{WB_API_BASE_URL}{url}',
        params=params,
        headers={
            'X-Mpstats-TOKEN': WB_API_TOKEN,
            'Content-Type': 'application/json',
        }
    ).json()


def get_data(product_id, dates_dict):
    response = run_request(
        method='GET',
        url=f'get/item/{product_id}/orders_by_region',
        params={
            'd1': dates_dict['start'],
            'd2': dates_dict['end'],
        }
    )
    return response


def value_or_zero(value):
    if value == 'NaN':
        return 0
    return value


if __name__ == '__main__':
    data_map = {}
    for pr_id in PRODUCT_IDS:
        object_id = str(pr_id)
        if not hasattr(data_map, object_id):
            data_map[object_id] = {}

        data = get_data(object_id, {'start': START_DATE, 'end': END_DATE})
        for data_date in data:
            for city_name in data[data_date]:
                data_map[object_id][city_name] = {
                    'sales': 0,
                    'balance': 0
                }

        for data_date in data:
            for city_name in data[data_date]:
                data_map[object_id][city_name]['sales'] += value_or_zero(data[data_date][city_name]['sales'])
                data_map[object_id][city_name]['balance'] += value_or_zero(data[data_date][city_name]['balance'])

    arr_to_render = []

    for product_id in data_map:
        for city in data_map[product_id]:
            arr_to_render.append({
                'ID': product_id,
                'Склад': city,
                'Sales': data_map[product_id][city]['sales'],
                'Balance': data_map[product_id][city]['balance'],
            })

    df = pd.DataFrame.from_records(arr_to_render, index='ID')

    print('Data frame ready')
    print(df)

    df.to_excel('results.xlsx')
