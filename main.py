import requests as rq
import json
import re
import sys
import pandas as pd

CATEGORY_URL = '/catalog/zhenshchinam/odezhda/dzhinsy-dzhegginsy'
START_DATE = '2023-08-07'
END_DATE = '2023-08-13'

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


def get_product_page(path, date, start, end):
    json_data = {}
    json_data['endRow'] = end
    json_data['startRow'] = start

    response = run_request(
        method='POST',
        url='get/category',
        json=json_data,
        params={
            'path': path,
            'd1': date,
            'd2': date,
        }
    )
    return response


def get_products_by_category_path(path, date):
    batch_size = 5000
    data = []
    print(f'Get products by path {path}, date: {date}')
    first_response = get_product_page(path, date, len(data), batch_size)

    total = first_response['total']
    print(f'Total products: {total}, date: {date}')
    data = data + first_response['data']

    while total > len(data):
        start = len(data)
        end = len(data) + batch_size
        print(f'Waiting product next chunk, start: {start}, end: {end}, date: {date}')
        product = get_product_page(path, date, start, end)
        data = data + product['data']

    print(f'Products successfully received, data count: {len(data)}, date: {date}')

    return data


if __name__ == '__main__':
    category = get_category_by_path(CATEGORY_URL)
    if category is None:
        print(f'Category by url: {CATEGORY_URL} not found')
        sys.exit(1)

    products_by_date = {}

    dates_range = pd.date_range(start=START_DATE, end=END_DATE)
    print(f'Dates: {dates_range}')
    for date in dates_range:
        formatted_date = date.strftime('%Y-%m-%d')
        products_by_date[formatted_date] = get_products_by_category_path(category['path'], formatted_date)

    data_frame = {}
    for date in products_by_date:
        data_frame[date] = reduce(lambda acc, cur: acc + cur['revenue'], products_by_date[date], 0)

    df = pd.DataFrame.from_dict({
        'Category': [category['path']],
        **data_frame
    }).set_index('Category')

    print('Data frame ready')
    print(df)

    df.to_excel('results.xlsx')
