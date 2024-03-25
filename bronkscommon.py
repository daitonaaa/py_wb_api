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


class MskladGateway:
    def __init__(self, auth_params):
        self.auth_params = auth_params

    def __request(self, method, url_path, data, params):
        try:
            return rq.request(
                method=method,
                url=f'https://api.moysklad.ru/{url_path}',
                params=params,
                data=data,
                headers={
                    'Authorization': f'Basic {self.auth_params["basic_auth"]}'
                }
            ).json()
        except Exception as err:
            print(err)
            print(traceback.format_exc())

    def get_all_stores(self):
        api_response = self.__request('GET', 'api/remap/1.2/entity/store', None, None)
        return api_response['rows'] if api_response else []

    def get_profit_by_products(self, ms_store_link, date_from, date_to):
        results = []
        params = {
            'momentFrom': date_from,
            'momentTo': date_to,
            'filter': f'store={ms_store_link}',
            'limit': 1000,
        }

        first_call = self.__request('GET', 'api/remap/1.2/report/profit/byproduct', None, params)
        results.extend(first_call['rows'])

        while first_call['meta']['size'] > len(results):
            second_call = self.__request(
                'GET', 'api/remap/1.2/report/profit/byproduct', None, {**params, 'offset': len(results)}
            )
            results.extend(second_call['rows'])

        return results

    def get_all_demand_by_store(self, ms_store_link, date_from, date_to):
        results = []
        params = {
            'filter': f'updated>={date_from};updated<={date_to};store={ms_store_link}',
            'limit': 1000,
        }

        first_call = self.__request('GET', 'api/remap/1.2/entity/demand', None, params)
        results.extend(first_call['rows'])

        while first_call['meta']['size'] > len(results):
            second_call = self.__request(
                'GET', 'api/remap/1.2/entity/demand', None, {**params, 'offset': len(results)}
            )
            results.extend(second_call['rows'])

        return results


user344mysqldb_creds = {
    'host': 'db.retrafic.ru',
    'user': 'user344',
    'password': '1d9dce46940f625466943eacc499718f',
    'database': 'user344',
    'port': 3306
}

user205mysqldb_creds = {
    'host': 'db.retrafic.ru',
    'user': 'user205',
    'password': 'b627e57c1b3bcfb7e4eb15be5be19019',
    'database': 'user205',
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

wb_api_creds = {
    'Aкс': {
        'auth_token': 'Bearer eyJhbGciOiJFUzI1NiIsImtpZCI6IjIwMjMxMDI1djEiLCJ0eXAiOiJKV1QifQ.eyJlbnQiOjEsImV4cCI6MTcxODc1NjY5NSwiaWQiOiIwMTM3NjM0My05NzFlLTQ3ZWUtYWU2ZC0yNGM4NDE4ZTdjNjYiLCJpaWQiOjg2NTQ4NDcsIm9pZCI6NjcwNzYxLCJzIjo1MTAsInNhbmRib3giOmZhbHNlLCJzaWQiOiJhZmQwNWNkZC0zYTg4LTRmNTEtYThlNi1lN2I2NDVhNGM1NTYiLCJ1aWQiOjg2NTQ4NDd9.MgFRxQ1kUTT4nRLr7grLwiqr5OQ2IlPRjk8odcnmBfyUsECjjCA0mP-eR-BBnhyKESWRzJWIir99H4uPeQ6msg'
    },
    'Онихимовский': {
        'auth_token': 'Bearer eyJhbGciOiJFUzI1NiIsImtpZCI6IjIwMjMxMDI1djEiLCJ0eXAiOiJKV1QifQ.eyJlbnQiOjEsImV4cCI6MTcxODc1NjcxNywiaWQiOiJmYzZmYjIxYy1mY2UxLTRlNjYtODM2Yy00MzNjMTNiZjg5ZmYiLCJpaWQiOjg2NTQ4NDcsIm9pZCI6ODkyMDU5LCJzIjo1MTAsInNhbmRib3giOmZhbHNlLCJzaWQiOiIzYjk3YWUzYi0zOGVmLTQ1NTktYTVkNC1jN2FkZDliNWZiMTUiLCJ1aWQiOjg2NTQ4NDd9.9UOLZkSM1FSu2cBUpCxt4sxHN57EqhALJM4Ss6EPW-ElHZREudLBCNrtXCjOAHUrAUMCqekkyNYrhpXaDybxLw'
    },
    'Чайковский': {
        'auth_token': 'Bearer eyJhbGciOiJFUzI1NiIsImtpZCI6IjIwMjMxMDI1djEiLCJ0eXAiOiJKV1QifQ.eyJlbnQiOjEsImV4cCI6MTcxODc1NjczOSwiaWQiOiIxZTNhMmQ3Ny1hMTQ3LTRmNzEtOWRmMC05YTE3NWRhZmZiZjUiLCJpaWQiOjg2NTQ4NDcsIm9pZCI6OTkzODUwLCJzIjo1MTAsInNhbmRib3giOmZhbHNlLCJzaWQiOiIzMmZkNmUxNC1hNDRlLTQyMTgtYWMwMy05YjRiZTZlNGIxMTkiLCJ1aWQiOjg2NTQ4NDd9.O-gnScDeLec-pEyCDvsfExMbmZY5wo_wt80zGTS9oliCxu7ahsB7MNa9oVZ-z67s0NKVJ1PBpDAEZEBVpNZZ6g'
    },
    'Малец': {
        'auth_token': 'Bearer eyJhbGciOiJFUzI1NiIsImtpZCI6IjIwMjMxMDI1djEiLCJ0eXAiOiJKV1QifQ.eyJlbnQiOjEsImV4cCI6MTcxODc1Njc1NiwiaWQiOiI2MGY0YzU3Yy1hZDdmLTRhZGMtYjBlNC0zZTk5YWUwNTNhZDQiLCJpaWQiOjExMDM3NTAxNCwib2lkIjoxMjY1Njg0LCJzIjo1MTAsInNhbmRib3giOmZhbHNlLCJzaWQiOiJmMGNkYmQ0Yi1mOTg3LTQ4ZjYtOWYyNy02ZTg3ZjVhZGNhZjIiLCJ1aWQiOjExMDM3NTAxNH0.7868glS1Dq1Kz_XDZ9XrpQi8B9jWFY_NUrFyVdmtrmIXRwxlvGw37roz6tLKhfJ7fl1_APhyO1eXJqGiQI_-jg'
    },
    'Чайковская': {
        'auth_token': 'Bearer eyJhbGciOiJFUzI1NiIsImtpZCI6IjIwMjMxMDI1djEiLCJ0eXAiOiJKV1QifQ.eyJlbnQiOjEsImV4cCI6MTcxODc1Njc5MSwiaWQiOiI0YzJlNjZmMS0yYmUyLTRlYTUtYjk2MC02ZmY3YmU4OGU1MDciLCJpaWQiOjEwOTkxNzU5Miwib2lkIjoxMjM2MjQzLCJzIjo1MTAsInNhbmRib3giOmZhbHNlLCJzaWQiOiI1YzFlM2QzZS04MWI5LTRlNzEtYTJhZS0zYTc1ZGViMDZjNDkiLCJ1aWQiOjEwOTkxNzU5Mn0.v4SpDEkYfn3qa6irKRjUpubsxL6W28o5Zq2niFbHmUWAb0MvnRLGDhu1ji5VKPwI4csRV6ukgmWliHAHA56ibg'
    },
    'Банишевский': {
        'auth_token': 'Bearer eyJhbGciOiJFUzI1NiIsImtpZCI6IjIwMjMxMDI1djEiLCJ0eXAiOiJKV1QifQ.eyJlbnQiOjEsImV4cCI6MTcxNzgxNzQxMiwiaWQiOiIzODg5ZWRiYi0zZTY2LTQxYzYtOTg2NS03ZDc2ZWRmMTRkYTQiLCJpaWQiOjg2NTQ4NDcsIm9pZCI6MTU0MzA3LCJzIjo1MTAsInNpZCI6IjY0ODQ4YWMyLWUxNjMtNDhkNS04ODE3LWVkY2VmYjZiNWNlZSIsInVpZCI6ODY1NDg0N30.gjUmXmoHjVHck3pu-cvlMd0fe6zr5zMVn5uCpfa4yzvZVOQpLyaZq-nRQ9PQJvlphIMk6jPtL28jwYosrl4bWw'
    },
    'ИП Пашкович': {
        'auth_token': 'Bearer eyJhbGciOiJFUzI1NiIsImtpZCI6IjIwMjMxMDI1djEiLCJ0eXAiOiJKV1QifQ.eyJlbnQiOjEsImV4cCI6MTcxNzEwOTI3MSwiaWQiOiJjNTQwMDdmNC1lMjlkLTQzNGUtYTcyZC1mMmUzMTIzMjczMzUiLCJpaWQiOjQzOTk0OTE2LCJvaWQiOjI2NzA3NCwicyI6NTEwLCJzaWQiOiI5MWUxNGY0Zi05MTNmLTQzYTktOTNjZC03NjZkOTQwYTRlZTAiLCJ1aWQiOjQzOTk0OTE2fQ.Iyxn2RIAywWodXEtLbcKfXolSS-1u5KSTTeNG7PnMybyy6UHCJDlQDVkztct-hG4B7QG29sUjNJnqp1Oa7PRAw'
    }
}

ms_api_base_creds = {
    'basic_auth': 'ZmxpcHdoaXBrb3JrQHBhc2hrb3ZpY2g0bGlmZTQ6QWJyYWhhbTE1MDUxOTkx'
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


def prepare_row_to_save(row, fields_to_sync, fields_formatters, field_transformers={}):
    dict_to_for_save = {}

    for field in row:
        if field in fields_to_sync:
            raw_value = row[field]
            value = fields_formatters[field](raw_value) if field in fields_formatters else raw_value
            cleaned_field = field.replace(".", "_").lower()
            cleaned_value = field_transformers[field](value) if field in field_transformers else value
            dict_to_for_save[cleaned_field] = cleaned_value

    return dict_to_for_save


def key_alias(base_dict, alias):
    result = {}
    for key in base_dict:
        result[f'{alias}.{key}'] = base_dict[key]

    return result


def format_num(raw):
    try:
        formatted_value = "{:10.2f}".format(raw).strip()
        return formatted_value
    except:
        return raw


def compose(fn_array):
    def fn(val):
        current_val = val
        for fn_callable in fn_array:
            current_val = fn_callable(current_val)

        return current_val

    return fn


def to_rub(raw):
    if raw:
        return raw / 100
    return raw
