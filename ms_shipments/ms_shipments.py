from bronkscommon import MskladGateway, ms_api_base_creds, prepare_row_to_save, compose, format_num, to_rub, Telegram, \
    MysqlDatabaseConnect, user205mysqldb_creds
import datetime
from pandas.tseries.offsets import Day

tg = Telegram('😊😊😊 ms_shipments')
date_str_format = '%Y-%m-%d %H:%M:%S'
prev_day = datetime.datetime.now() - Day(1)

start_of_day = datetime.datetime(prev_day.year, prev_day.month, prev_day.day, 0, 0, 0)
end_of_day = datetime.datetime(prev_day.year, prev_day.month, prev_day.day, 23, 59, 59)

date_start = start_of_day.strftime(date_str_format)
date_end = end_of_day.strftime(date_str_format)

if __name__ == '__main__':
    ms_gateway = MskladGateway(ms_api_base_creds)
    db = MysqlDatabaseConnect(user205mysqldb_creds)

    try:
        stores = ms_gateway.get_all_stores()

        for store in stores:
            tg.send_message(f'start get data for store: {store["name"]}, dates: {date_start} - {date_end}')
            items = ms_gateway.get_all_demand_by_store(store['meta']['href'], date_start, date_end)

            for item in items:
                to_save_db = prepare_row_to_save({
                    **item,
                    'store.name': store["name"],
                    'store.meta.href': store['meta']['href'],
                }, [
                    'store.meta.href',
                    'store.name',
                    'sum',
                    'updated',
                    'moment',
                    'name'
                ], {}, {
                    'sum': compose([to_rub, format_num]),
                })

                db.save_row('ms_shipments', to_save_db)

            tg.send_message(f'saved data count: {len(items)}')
    except Exception as err:
        tg.error_traceback()
    finally:
        db.destroy()
        tg.send_message('end')
