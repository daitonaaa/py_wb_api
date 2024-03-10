from bronkscommon import OzonGateway, Telegram, MysqlDatabaseConnect, user344mysqldb_creds, ozon_api_creds, \
    prepare_row_to_save

tg = Telegram('ozon warehouse 🛒🛒🛒')

if __name__ == '__main__':
    db = MysqlDatabaseConnect(user344mysqldb_creds)
    try:
        for company_name in ozon_api_creds:
            ozon_gateway = OzonGateway(ozon_api_creds[company_name])
            tg.send_message(f'getting data for company {company_name}')
            stocks = ozon_gateway.get_all_stock_on_warehouses()
            tg.send_message(f'getting data for company {company_name}, success, total: {len(stocks)}')

            for result_item in stocks:
                row_to_db_save = prepare_row_to_save({
                    **result_item,
                    'companyname': company_name
                },
                    ['companyname', 'sku', 'warehouse_name', 'item_code', 'item_name',
                     'promised_amount', 'free_to_sell_amount', 'reserved_amount'],
                    {})

                db.save_row('ozon_warehouse', row_to_db_save)

            tg.send_message(f'data from company: {company_name} saved')
    except:
        tg.error_traceback()
    finally:
        db.destroy()
