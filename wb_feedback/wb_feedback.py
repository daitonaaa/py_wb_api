import requests
from bronkscommon import key_alias, Telegram, MysqlDatabaseConnect, user205mysqldb_creds, prepare_row_to_save, wb_api_creds

tg = Telegram('wb_feedbacks ✨✨✨')


def get_feedbacks(skip, date_from, is_answered, authorization):
    url = f"https://feedbacks-api.wildberries.ru/api/v1/feedbacks?isAnswered={is_answered}&take=5000&skip={skip}&dateFrom={date_from}"
    payload = {}
    headers = {
        'Authorization': authorization,
    }

    return requests.request("GET", url, headers=headers, data=payload).json()['data']


if __name__ == '__main__':
    db = MysqlDatabaseConnect(user205mysqldb_creds)
    tg.send_message('start')

    try:
        for company_name in wb_api_creds:
            unix_timestamp = 1690840801
            answered = [True, False]
            raw_data = []

            for is_answered in answered:
                company_api_auth = wb_api_creds[company_name]['auth_token']
                tg.send_message(f'get company feedback {company_name}, is_answered: {is_answered}')
                first_call = get_feedbacks(0, unix_timestamp, is_answered, company_api_auth)
                first_call_data = first_call['feedbacks']
                raw_data.extend(first_call_data)

                if len(first_call_data) >= 5000:
                    while True:
                        tg.send_message(f'get chunk company ({company_name}), current rows size: {len(raw_data)}')
                        call_2 = get_feedbacks(len(raw_data), unix_timestamp, is_answered, company_api_auth)
                        call_2_data = call_2['feedbacks']
                        raw_data.extend(call_2_data)

                        if len(call_2_data) < 5000:
                            break

            tg.send_message(f'start save data for {company_name}, data count: {len(raw_data)}')
            for data_item in raw_data:
                row_to_save_db = {
                    **data_item,
                    **key_alias(data_item['productDetails'], 'productDetails'),
                    'feedback_id': data_item['id'],
                    'answerText': data_item['answer']['text'] if data_item['answer'] is not None else '',
                    'photoLinks': ", ".join(map(lambda photoDict: photoDict['fullSize'], data_item['photoLinks'])) if data_item['photoLinks'] is not None else '',
                    'companyname': company_name,
                }

                prepared_row = prepare_row_to_save(
                    row_to_save_db,
                    [
                        'feedback_id',
                        'answerText',
                        'text',
                        'productValuation',
                        'createdDate',
                        'photoLinks',
                        'userName',
                        'matchingSize',
                        'isAbleSupplierFeedbackValuation',
                        'supplierFeedbackValuation',
                        'isAbleSupplierProductValuation',
                        'supplierProductValuation',
                        'isAbleReturnProductOrders',
                        'returnProductOrdersDate',
                        'productDetails.imtId',
                        'productDetails.nmId',
                        'productDetails.productName',
                        'productDetails.supplierArticle',
                        'productDetails.supplierName',
                        'productDetails.brandName',
                        'productDetails.size',
                        'companyname',
                    ],
                    {}
                )

                db.save_row('wb_feedback', prepared_row)

            tg.send_message(f'success saved data for {company_name}, data count: {len(raw_data)}')

            tg.send_message(f'company ({company_name}) end')
    except Exception as err:
        tg.error_traceback()
    finally:
        db.destroy()
