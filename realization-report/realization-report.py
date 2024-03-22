import requests
import traceback
import time
from urllib.parse import quote
import mysql.connector
import datetime
from pandas.tseries.offsets import Day

DATE_STR_FORMAT = '%d.%m.%Y'

DATE_FROM = (datetime.datetime.now() - Day(8)).strftime(DATE_STR_FORMAT)
DATE_TO = datetime.datetime.now().strftime(DATE_STR_FORMAT)
DATA_LIMIT = 5
DEFAULT_CHUNK_SLEEP_SECONDS = 15
DB_TABLE_NAME = 'weekly_finance_2'

db_connection = mysql.connector.connect(
    host="db.retrafic.ru",
    user="user344",
    password="1d9dce46940f625466943eacc499718f",
    database="user344",
    port=3306
)

cursor = db_connection.cursor()


def db_find_by_report_id(report_id, company_name):
    cursor.execute(f"SELECT * FROM {DB_TABLE_NAME} where reportid = '{report_id}' and companyname = '{company_name}'")
    return cursor.fetchall()


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


def format_num(raw):
    try:
        formatted_value = "{:10.2f}".format(raw)
        return formatted_value
    except:
        return raw


def save_rows_to_retrafic(rows_value, company_name):
    fields_to_sync = [
        'detailsCount',
        'type',
        'dateFrom',
        'dateTo',
        'createDate',
        'totalSale',
        'avgSalePercent',
        'forPay',
        'deliveryRub',
        'paidStorageSum',
        'paidAcceptanceSum',
        'paidWithholdingSum',
        'bankPaymentSum',
        'bankPaymentStatusId',
        'bankPaymentStatusName',
        'bankPaymentStatusDescription',
        'penalty',
        'penaltyLogistics',
        'penaltyWithoutLogistics',
        'additionalPayment',
        'currency',
        'banReason',
        'bankPaymentWithoutPrev',
        'supplierFinanceId',
        'supplierFinanceName',
        'id'
    ]
    field_aliases = {
        'id': 'reportid'
    }
    fields_formatters = {
        '': format_num,
    }
    for row_dict in rows_value:
        exist_db_report = db_find_by_report_id(row_dict['id'], company_name)
        if len(exist_db_report) > 0:
            continue

        dict_to_for_save = {
            'companyname': company_name,
        }
        for field in row_dict:
            if field in fields_to_sync:
                raw_value = row_dict[field]
                value = fields_formatters[field](raw_value) if field in fields_formatters else raw_value
                field_after_aliased = field_aliases[field] if field in field_aliases else field
                cleaned_field_name = field_after_aliased.replace(".", "_").lower()
                cleaned_value = value
                dict_to_for_save[cleaned_field_name] = cleaned_value

        db_save_row(dict_to_for_save)


def send_telegram_message(text_message):
    chat_id = -4020643298
    tg_token = '6236028980:AAHSNJaZQ3wFmtiviAvK26ABycfpG9vQz40'
    message = quote(f'WB realization report 💵💵💵: {text_message}')

    requests.request(
        method='GET',
        url=f'https://api.telegram.org/bot{tg_token}/sendMessage?chat_id={chat_id}&text={message}'
    )


def get_request(date_from, date_to, skip, limit):
    # url = f"https://seller-weekly-report.wildberries.ru/ns/realization-reports/suppliers-portal-analytics/api/v1/reports?dateFrom={date_from}&dateTo={date_to}&limit={limit}&searchBy=&skip={skip}&type=2"
    url = f"https://seller-weekly-report.wildberries.ru/ns/reports/seller-wb-balance/api/v1/reports-weekly?dateFrom={date_from}dateTo={date_to}&limit={limit}&searchBy=&skip={skip}&type=5"

    headers = {
        'Cookie': '_wbauid=1308642931691750188; ___wbu=16089b21-20df-41ca-91a4-4a37ab956801.1691750189; BasketUID=ecaa508a18914a6084e322e0a320c6db; external-locale=ru; __zzatw-wb=MDA0dBA=Fz2+aQ==; locale=ru; __zzatw-wb=MDA0dBA=Fz2+aQ==; x-supplier-id=f0cdbd4b-f987-48f6-9f27-6e87f5adcaf2; x-supplier-id-external=f0cdbd4b-f987-48f6-9f27-6e87f5adcaf2; ___wbs=3d57584c-ccb6-4361-9c04-566b15e4ca2c.1709825222; __catalogOptions=Sort%3APopular%26CardSize%3Ac516x688; current_feature_version=4DA863B2-C36B-4FC7-8B37-F26C86640593; wbx-validation-key=1308bf74-3452-4b1e-995e-03b87f82ae98; WBTokenV3=eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9.eyJpYXQiOjE3MDk4MjUyMjgsInZlcnNpb24iOjIsInVzZXIiOiI3Mjk4MzEyNyIsInNoYXJkX2tleSI6IjgiLCJjbGllbnRfaWQiOiJzZWxsZXItcG9ydGFsIiwic2Vzc2lvbl9pZCI6ImNmOTIwMTUxODg4YjQ3MTY5ZmJjN2VkYWE3MjlhMThjIiwidXNlcl9yZWdpc3RyYXRpb25fZHQiOjE2OTUwNTU3NjgsInZhbGlkYXRpb25fa2V5IjoiNzFkMTZmNmRkNDEwMWM5MGNmM2JmZDMwNmQxMzIyMzQ4NmY3YThlMjhjOWFkY2IxZWU3YjIzYzUwYzkyZjMxNSIsInBob25lIjoiVHB6NnlGTitVSHdhRnF5K09UOHZ4QT09In0.mmP3sBFESBFLtGJ9MfWRBzTlBLt_MLT3ByDbbzSS0Rt4We_lXwoCAgPcOpAi68SZ1uPjmEJgfhOimGA7rM90HyFhy7mp0y2moSeYvv3SFFcztTlWpk3bFWbJnziMOx5WdbtSjvd6AwmNuLabUYWFOE4deJ5sYE0LlUP_Qd2-5N_mfl6PsMNGjG-Qm8TZg1nhWvVoFbLDFIZC3cmysTLK8mFLW6QRki3yTEVnpFTuQsARuWVXzEPiO9wu6vUDKFjNED9_cV3w80fAmk4Bfg7u0P8GPzZWLhJwdl6-FuQpvfOQ0Pp5fR-FJY5Zham_-wcmhL4fHEUsBOlv6baCPRhdww; cfidsw-wb=SBKPsdEnEaP06VeilkfEM1qZFwdBZGijkhgLQw9f5BvlE2ubOuuBDVQavXdml0CtmrYrOsvuDUjUAC+CSAW8qu502aDT6EVzKpHQzdrpRw10x1V2vL7Tp5u7vKYqq6loj71j0TAlgrvjObp8CI41mcIqMjkb2VyF5xEJ7Ok=; cfidsw-wb=SBKPsdEnEaP06VeilkfEM1qZFwdBZGijkhgLQw9f5BvlE2ubOuuBDVQavXdml0CtmrYrOsvuDUjUAC+CSAW8qu502aDT6EVzKpHQzdrpRw10x1V2vL7Tp5u7vKYqq6loj71j0TAlgrvjObp8CI41mcIqMjkb2VyF5xEJ7Ok=',
        'Origin': 'https://seller.wildberries.ru',
        'Referer': 'https://seller.wildberries.ru/',
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
    }

    request_result = requests.request("GET", url, headers=headers)

    try:
        return request_result.json()
    except:
        return {'is_error': True, 'status_code': request_result.status_code}


if __name__ == '__main__':
    try:
        company_name = 'Малец'
        send_telegram_message(f'start for company {company_name}, for dates {DATE_FROM} - {DATE_TO}')
        results_arr = []
        first_response = get_request(DATE_FROM, DATE_TO, 0, DATA_LIMIT)
        total_records_count = first_response['data']['count']
        send_telegram_message(f'total data count: {total_records_count}')
        results_arr.extend(first_response['data']['reports'])

        while total_records_count > len(results_arr):
            print(f'start data fetching, first sleep {DEFAULT_CHUNK_SLEEP_SECONDS} s')
            time.sleep(DEFAULT_CHUNK_SLEEP_SECONDS)
            response = get_request(DATE_FROM, DATE_TO, len(results_arr), DATA_LIMIT)
            if 'is_error' in response:
                send_telegram_message(f'received error, status_code: {response["status_code"]}')
                continue

            results_arr.extend(response['data']['reports'])

        send_telegram_message(f'save rows for {company_name}')
        save_rows_to_retrafic(results_arr, company_name)
    except Exception as err:
        send_telegram_message(f'❌ ERR: {err}')
        send_telegram_message(traceback.format_exc())
    finally:
        cursor.close()
        db_connection.close()

