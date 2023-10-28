import requests as rq
import json
import re
import pandas as pd
from pandas.tseries.offsets import Day, MonthEnd, Hour, Minute, Second

CATEGORY_URLS = [
    # '/catalog/zhenshchinam/odezhda/dzhinsy-dzhegginsy',
    '/catalog/zootovary/dlya-loshadey'
]
START_DATE = '2023-08-29'
END_DATE = '2023-08-31'

# FREQ = 'm'
# FREQ = 'w'
FREQ = 'd'


def get_dates_range():
    result = []

    freq = 'MS'
    if FREQ == 'w':
        freq = 'W-MON'
    if FREQ == 'd':
        freq = 'D'

    increment_start_date_value = MonthEnd(1)
    if FREQ == 'w':
        increment_start_date_value = Day(6)
    if FREQ == 'd':
        increment_start_date_value = Hour(23) + Minute(59) + Second(59)

    start_dates = pd.date_range(start=START_DATE, end=END_DATE, freq=freq)
    for st in start_dates:
        result.append({
            'start': st,
            'end': st + increment_start_date_value,
        })
    return result


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

    arr_to_render = []
    for result_item in results:
        row = {
            'category': result_item,
        }
        for date_key in data_frame:
            row[f'revenue {date_key}'] = reduce(lambda acc, cur: acc + cur['revenue'], results[result_item][date_key], 0)
            row[f'sales {date_key}'] = reduce(lambda acc, cur: acc + cur['sales'], results[result_item][date_key], 0)
        arr_to_render.append(row)

    # sort columns
    col_data = []
    col_sales = []
    col_revenue = []
    for col in arr_to_render[0].keys():
        if r"revenue 2" in col:
            col_revenue.append(col)
        elif r"sales 2" in col:
            col_sales.append(col)
        else:
            col_data.append(col)

    columns = col_data + col_sales + col_revenue

    df = pd.DataFrame.from_records(arr_to_render, index='category', columns=columns)

    print('Data frame ready')
    print(df)

    df.to_excel('results.xlsx')
