import requests as rq
import json
import re
import pandas as pd
from pandas.tseries.offsets import Day, MonthEnd

CATEGORY_URLS = [
    '/catalog/zhenshchinam/odezhda/dzhinsy-dzhegginsy',
    '/catalog/zootovary/dlya-loshadey'
]
START_DATE = '2023-06-01'
END_DATE = '2023-08-31'

FREQ = 'm'
# FREQ = 'w'

# todo add freq by day

# don't touch this
WB_API_TOKEN = '64d4cb0f3e13e5.048170039dcc9d3bc2b4f5ef8ea45510cb33335c'
WB_API_BASE_URL = 'https://mpstats.io/api/wb/'
CAT_DUMP_FILENAME = 'categories_dump.json'


def reduce(function, iterable, initializer=None):
    it = iter(iterable)
    if initializer is None:
        value = next(it)
    else:
        value = initializer
    for element in it:
        value = function(value, element)
    return value


def run_request(method, url, params, json):
    return rq.request(
        method=method,
        url=f'{WB_API_BASE_URL}{url}',
        json=json,
        params=params,
        headers={
            'X-Mpstats-TOKEN': WB_API_TOKEN,
            'Content-Type': 'application/json',
        }
    ).json()


def save_cat_dumb(data):
    with open(CAT_DUMP_FILENAME, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False)


def get_cat_dump():
    try:
        with open(CAT_DUMP_FILENAME, encoding='utf-8') as fh:
            data = json.load(fh)
        return data
    except:
        return None


def get_categories():
    cache = get_cat_dump()
    if cache:
        return cache

    response = run_request(method='GET', url='get/categories', params=None, json=None)
    save_cat_dumb(response)
    return response


def get_category_by_path(path):
    categories = get_categories()
    for cat in categories:
        name_wot_params = re.sub(r"\?.+$", 'gm', cat['url'])
        if name_wot_params == path:
            return cat
    return None


def get_product_page(path, dates_dict, start, end):
    json_data = {}
    json_data['endRow'] = end
    json_data['startRow'] = start

    response = run_request(
        method='POST',
        url='get/category',
        json=json_data,
        params={
            'path': path,
            'd1': dates_dict['start'],
            'd2': dates_dict['end'],
        }
    )
    return response


def get_products_by_category_path(path, dates_dict):
    batch_size = 5000
    data = []
    print(f'Get products by path {path}, dates: {dates_dict}')
    first_response = get_product_page(path, dates_dict, len(data), batch_size)

    total = first_response['total']
    print(f'Total products: {total}, dates: {dates_dict}')
    data = data + first_response['data']

    while total > len(data):
        start = len(data)
        end = len(data) + batch_size
        print(f'Waiting product next chunk, start: {start}, end: {end}, dates: {dates_dict}')
        product = get_product_page(path, dates_dict, start, end)
        data = data + product['data']

    print(f'Products successfully received, data count: {len(data)}, dates: {dates_dict}')

    return data


def get_dates_range():
    result = []
    freq = 'MS' if FREQ == 'm' else 'W-MON'
    start_dates = pd.date_range(start=START_DATE, end=END_DATE, freq=freq)
    for st in start_dates:
        result.append({
            'start': st,
            'end': st + (MonthEnd(1) if FREQ == 'm' else Day(6)),
        })
    return result


if __name__ == '__main__':
    results = {}
    data_frame = {}

    for category_url in CATEGORY_URLS:
        category = get_category_by_path(category_url)
        if category is None:
            print(f'Category by url: {category_url} not found')
            continue

        products_by_date = {}

        dates_range = get_dates_range()
        print(f'Dates: {dates_range}')
        for range_dict in dates_range:
            dates = {
                'start': range_dict['start'].strftime('%Y-%m-%d'),
                'end': range_dict['end'].strftime('%Y-%m-%d')
            }
            print(dates)
            dates_key = f'{dates["start"]}/{dates["end"]}'
            data_frame[dates_key] = []
            products_by_date[dates_key] = get_products_by_category_path(
                category['path'], dates)

        results[category['path']] = products_by_date

    unique_categories = []

    for result_item in results:
        unique_categories.append(result_item)

    for date_key in data_frame:
        for result_item in results:
            data_frame[date_key].append(
                reduce(lambda acc, cur: acc + cur['revenue'], results[result_item][date_key], 0))

    df = pd.DataFrame.from_dict({
        'Category': unique_categories,
        **data_frame
    }).set_index('Category')

    print('Data frame ready')
    print(df)

    df.to_excel('results.xlsx')

