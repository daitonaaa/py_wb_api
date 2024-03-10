import os
import requests as rq
import pandas as pd
from urllib.parse import quote
import datetime
import traceback
import ftplib

RETRAEFIC_WEBHOOK_SAVE = 'https://retrafic.ru/api/integrations/save/df50bd45966c3b8ef5e8cde071389327'

def send_telegram_message(text_message):
    chat_id = -4020643298
    tg_token = '6236028980:AAHSNJaZQ3wFmtiviAvK26ABycfpG9vQz40'
    message = quote(f'weekly_finance ⏰⏰⏰: {text_message}')

    rq.request(
        method='GET',
        url=f'https://api.telegram.org/bot{tg_token}/sendMessage?chat_id={chat_id}&text={message}'
    )


def format_float(raw):
    try:
        formatted_value = "{:10.2f}".format(raw)
        return formatted_value
    except:
        return raw


def format_date(raw):
    rus_date_obj = datetime.datetime.strptime(raw, '%d.%m.%Y %H:%M:%S')
    return rus_date_obj.strftime('%Y-%m-%d %H:%M:%S')


def raise_if_api_err(api_result):
    if 'is_error' in api_result:
        raise Exception(f'{api_result["status_code"]}:{api_result["reason"]}')
    if 'code' in api_result and api_result['code'] >= 400:
        raise Exception(api_result["message"])


def save_rows_to_retrafic(rows_value):
    fields_to_sync = [
        'numberReportWb',
        'accountName',
        'dateFrom',
        'totalSale',
        'forPay',
        'delivery_Rub',
        'penalty',
        'additionalPayment',
        'paidStorageSum',
        'paidAcceptanceSum',
        'paidWithholdingSum',
        'penaltyLogistics',
        'penaltyWithoutLogistics',
        'bankPaymentSum',
        'bankPaymentWithoutPrev',
        'currentStatusDocumentid',
        'currentStatusDocumentname',
        'detailsCount',
        'type'
    ]
    fields_formatters = {
        'dateFrom': format_date,
        'totalSale': format_float,
        'forPay': format_float,
        'delivery_Rub': format_float,
        'penalty': format_float,
        'additionalPayment': format_float,
        'paidStorageSum': format_float,
        'paidAcceptanceSum': format_float,
        'paidWithholdingSum': format_float,
        'penaltyLogistics': format_float,
        'penaltyWithoutLogistics': format_float,
        'bankPaymentSum': format_float,
        'bankPaymentWithoutPrev': format_float,
    }
    for row_dict in rows_value:
        query = ''
        for field in row_dict:
            if field in fields_to_sync:
                raw_value = row_dict[field]
                value = fields_formatters[field](raw_value) if field in fields_formatters else raw_value
                query += f'{field.replace(".", "_").lower()}={quote(str(value))}&'

        url_to_save = f'{RETRAEFIC_WEBHOOK_SAVE}?{query}'
        request_results = rq.request(method='GET', url=url_to_save)
        raise_if_api_err(request_results.json())


def save_cursor(new_cursor):
    with open('wb_otch_cursor.txt', 'w') as f:
        # Write some text to the file
        f.write(str(new_cursor))


def get_cursor():
    try:
        with open('wb_otch_cursor.txt', 'r') as f:
            # Read the contents of the file into a variable
            return int(f.read())
    except FileNotFoundError:
        return 0


def get_raw_field_name_alias(name):
    field_aliases = [
        ['НомерОтчетаВБ', 'numberReportWb'],
        ['УчетнаяЗапись', 'accountName']
    ]

    for field in field_aliases:
        if field[0] == name:
            return field[1]

    return None


if __name__ == '__main__':
    send_telegram_message('start')
    ftp = ftplib.FTP("srv4.mp-raketa.ru")
    ftp.login("u_ftp_10", "Z31lJWemA2")
    ftp.cwd('/')
    temp_file = 'otch.xlsx'

    try:
        with open(temp_file, 'wb') as f:
            ftp.retrbinary('RETR otch.xlsx', f.write)

        df = pd.read_excel(temp_file)
        headers = df.values[2]
        data = df.values[3:]

        rows = []
        cursor = get_cursor()
        send_telegram_message(f'current cursor {cursor}, total_data_count: {len(data)}, new_data_count: {len(data) - cursor}')
        for data_row_index, data_row in enumerate(data):
            if cursor > data_row_index:
                continue

            row_dict = {}
            for col_index, header_item in enumerate(headers):
                if header_item and str(header_item) != 'nan':
                    raw_data = data_row[col_index]
                    field_name = get_raw_field_name_alias(header_item) if get_raw_field_name_alias(
                        header_item) is not None else header_item
                    row_dict[field_name] = None if str(raw_data) == 'nan' else raw_data
            rows.append(row_dict)

        save_rows_to_retrafic(rows)
        send_telegram_message(f'saved new data count: {len(rows)}')
        save_cursor(len(data))
    except Exception as err:
        send_telegram_message(f'error ❌ ERR: {err}')
        send_telegram_message(traceback.format_exc())
    finally:
        # Close the FTP connection and delete the temporary file
        ftp.quit()
        os.remove(temp_file)
