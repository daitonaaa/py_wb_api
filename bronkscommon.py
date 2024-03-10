import json

import requests as rq
from urllib.parse import quote
import traceback
import mysql.connector


class Telegram:
    def __init__(self, name, is_enabled=True):
        self.name = name
        self.is_enabled = is_enabled

    def send_message(self, text_message):
        if self.is_enabled:
            chat_id = -4020643298
            tg_token = '6236028980:AAHSNJaZQ3wFmtiviAvK26ABycfpG9vQz40'
            message = quote(f'{self.name}: {text_message}')

            rq.request(
                method='GET',
                url=f'https://api.telegram.org/bot{tg_token}/sendMessage?chat_id={chat_id}&text={message}'
            )
        else:
            print(f'tg mock: {self.name}: {text_message}')

    def error_traceback(self):
        self.send_message(traceback.format_exc())


class OzonGateway:
    def __init__(self, auth_params):
        self.auth_params = auth_params

    def __request(self, method, url_path, data, params):
        try:
            return rq.request(
                method=method,
                url=f'https://api-seller.ozon.ru/{url_path}',
                params=params,
                data=data,
                headers={
                    'Client-Id': self.auth_params['client_id'],
                    'api-key': self.auth_params['api_key'],
                }
            ).json()
        except Exception as err:
            print(err)
            print(traceback.format_exc())

    def get_transactions(self, date_start, date_end, page):
        return self.__request('POST', 'v3/finance/transaction/list', json.dumps({
            "filter": {
                "date": {
                    "from": date_start,
                    "to": date_end,
                }
            },
            "page": page,
            "page_size": 1000
        }), {})

    def get_all_transactions(self, date_start, date_end):
        results = []
        cur_page = 1

        first_call_data = self.get_transactions(date_start, date_end, 1)
        results.extend(first_call_data['result']['operations'])

        if first_call_data['result']['page_count'] > cur_page:
            while first_call_data['result']['page_count'] > cur_page:
                cur_page += 1
                call_data = self.get_transactions(date_start, date_end, cur_page)
                results.extend(call_data['result']['operations'])

        return results

    def get_stock_on_warehouses(self, offset=0):
        return self.__request('POST', 'v2/analytics/stock_on_warehouses', json.dumps({
            "limit": 1000,
            "offset": offset,
            "warehouse_type": "ALL"
        }), {})

    def get_all_stock_on_warehouses(self):
        results = []
        current_offset = 0

        first_call_data = self.get_stock_on_warehouses(current_offset)
        rows_response = first_call_data['result']['rows']
        results.extend(rows_response)

        if len(rows_response) >= 1000:
            while True:
                current_offset += 1000
                rows_response_2 = self.get_stock_on_warehouses(current_offset)['result']['rows']
                results.extend(rows_response_2)

                if len(rows_response_2) < 1000:
                    break

        return results


user344mysqldb_creds = {
    'host': 'db.retrafic.ru',
    'user': 'user344',
    'password': '1d9dce46940f625466943eacc499718f',
    'database': 'user344',
    'port': 3306
}

ozon_api_creds = {
    'Акс': {
        'client_id': '426332',
        'api_key': '0348be24-ef7d-4655-afad-eeb95b8e64d5'
    },
    'Банишевский': {
        'client_id': '132081',
        'api_key': 'deadeb0b-ed93-4f15-88d3-4a746a7f2398'
    },
    'Онихимовский': {
        'client_id': '1686713',
        'api_key': 'fcdfdff5-af03-4b67-a5f8-76d9f59d773a'
    },
    'Пашкович': {
        'client_id': '307648',
        'api_key': '561b7154-6c61-4e5c-bd96-973b1137019c'
    }
}


class MysqlDatabaseConnect:
    def __init__(self, params):
        self.db_connection = mysql.connector.connect(
            host=params['host'],
            user=params['user'],
            password=params['password'],
            database=params['database'],
            port=params['port']
        )

        self.cursor = self.db_connection.cursor()

    def save_row(self, table, row):
        fields_arr = []
        for field in row:
            fields_arr.append(field)

        sql_query = f"INSERT INTO {table} ({', '.join(fields_arr)}) VALUES ({', '.join(['%s' for item in fields_arr])})"

        values = ()
        for field in row:
            values = values + (row[field],)

        self.cursor.execute(sql_query, values)
        self.db_connection.commit()

    def destroy(self):
        self.cursor.close()
        self.db_connection.close()

    def get_max_id_by_table(self, table_name):
        self.cursor.execute(f"SELECT MAX(id) FROM {table_name}")
        return self.cursor.fetchone()


def prepare_row_to_save(row, fields_to_sync, fields_formatters):
    dict_to_for_save = {}

    for field in row:
        if field in fields_to_sync:
            raw_value = row[field]
            value = fields_formatters[field](raw_value) if field in fields_formatters else raw_value
            cleaned_field = field.replace(".", "_").lower()
            cleaned_value = value
            dict_to_for_save[cleaned_field] = cleaned_value

    return dict_to_for_save


def key_alias(base_dict, alias):
    result = {}
    for key in base_dict:
        result[f'{alias}.{key}'] = base_dict[key]

    return result
