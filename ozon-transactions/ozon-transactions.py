from bronkscommon import OzonGateway, Telegram, MysqlDatabaseConnect, user344mysqldb_creds, key_alias, \
    prepare_row_to_save, ozon_api_creds

START_DATE = '2024-01-01T00:00:00.000Z'
END_DATE = '2024-01-31T23:59:59.000Z'

tg = Telegram('ozon transactions 📝📝📝')

if __name__ == '__main__':
    db = MysqlDatabaseConnect(user344mysqldb_creds)
    tg.send_message(f'start, from {START_DATE} to {END_DATE}')

    try:
        for company_name in ozon_api_creds:
            tg.send_message(f'getting data from company: {company_name}')
            ozon_gateway = OzonGateway(ozon_api_creds[company_name])

            transactions = ozon_gateway.get_all_transactions(START_DATE, END_DATE)
            tg.send_message(
                f'getting data from company: {company_name} success, total transactions: {len(transactions)}')

            for transaction in transactions:
                to_save_db = prepare_row_to_save({
                    **transaction,
                    **key_alias(transaction['posting'], 'posting'),
                    'companyname': company_name
                }, [
                    'operation_id',
                    'operation_type',
                    'operation_date',
                    'operation_type_name',
                    'delivery_charge',
                    'return_delivery_charge',
                    'accruals_for_sale',
                    'sale_commission',
                    'amount',
                    'type',
                    'posting.delivery_schema',
                    'posting.order_date',
                    'posting.posting_number',
                    'posting.warehouse_id',
                    'companyname'
                ], {})

                db.save_row('ozon_transactions', to_save_db)
                transaction_db_id = int(db.get_max_id_by_table('ozon_transactions')[0])

                for item in transaction['items']:
                    to_save_item = prepare_row_to_save({
                        **item,
                        'transaction_id': transaction_db_id
                    }, [
                        'name',
                        'sku',
                        'transaction_id',
                    ], {})

                    db.save_row('ozon_transactions_items', to_save_item)

                for service in transaction['services']:
                    to_save_service = prepare_row_to_save({
                        **service,
                        'transaction_id': transaction_db_id
                    }, [
                        'name',
                        'price',
                        'transaction_id',
                    ], {})

                    db.save_row('ozon_transactions_services', to_save_service)

            tg.send_message(
                f'data from company: {company_name} saved')

        tg.send_message('end')
    except:
        tg.error_traceback()
    finally:
        db.destroy()
