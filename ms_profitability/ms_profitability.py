from bronkscommon import MskladGateway, ms_api_base_creds, Telegram, prepare_row_to_save, MysqlDatabaseConnect, \
    key_alias, user205mysqldb_creds, compose, format_num, to_rub
from pandas.tseries.offsets import Hour, Minute, Second, Day
import pandas as pd
import datetime

date_str_format = '%Y-%m-%d %H:%M:%S'
date_str_format_short = '%Y-%m-%d'
tg = Telegram('⭐⭐⭐ ms_profitability')

current_date = datetime.datetime.now()
days_to_subtract = (current_date.weekday() + 1) % 7 + 6
previous_monday = current_date - datetime.timedelta(days=days_to_subtract)
previous_end_of_week = previous_monday + Day(6)

date_start_str = previous_monday.strftime(date_str_format_short)
date_end_str = previous_end_of_week.strftime(date_str_format_short)


def get_dates_range():
    result = []

    increment_start_date_value = Day(6) + Hour(23) + Minute(59) + Second(59)
    start_dates = pd.date_range(start=date_start_str, end=date_end_str, freq='W-MON')
    for st in start_dates:
        end_date_val = st + increment_start_date_value
        result.append({
            'startApi': st.strftime(date_str_format),
            'endApi': end_date_val.strftime(date_str_format),
        })
    return result


if __name__ == '__main__':
    ms_gateway = MskladGateway(ms_api_base_creds)
    db = MysqlDatabaseConnect(user205mysqldb_creds)

    try:
        stores = ms_gateway.get_all_stores()

        for range_item in get_dates_range():
            date_start = range_item['startApi']
            date_end = range_item['endApi']

            for store in stores:
                tg.send_message(f'start get data for store: {store["name"]}, dates: {date_start} - {date_end}')
                profits = ms_gateway.get_profit_by_products(store['meta']['href'], date_start, date_end)

                for profit in profits:
                    to_save_db = prepare_row_to_save({
                        **profit,
                        **key_alias(profit['assortment'], 'assortment'),
                        **key_alias(profit['assortment']['meta'], 'assortment.meta'),
                        'store.name': store["name"],
                        'store.meta.href': store['meta']['href'],
                        'date_start': date_start,
                        'date_end': date_end,
                    }, [
                        'store.meta.href',
                        'assortment.article',
                        'assortment.meta.href',
                        'store.name',
                        'assortment.name',
                        'assortment.code',
                        'sellQuantity',
                        'sellPrice',
                        'sellCost',
                        'sellSum',
                        'sellCostSum',
                        'returnQuantity',
                        'returnPrice',
                        'returnCost',
                        'returnSum',
                        'returnCostSum',
                        'profit',
                        'margin',
                        'date_start',
                        'date_end',
                    ], {}, {
                        'sellPrice': compose([to_rub, format_num]),
                        'sellCost': compose([to_rub, format_num]),
                        'sellSum': compose([to_rub, format_num]),
                        'sellCostSum': compose([to_rub, format_num]),
                        'returnPrice': compose([to_rub, format_num]),
                        'returnCost': compose([to_rub, format_num]),
                        'returnSum': compose([to_rub, format_num]),
                        'returnCostSum': compose([to_rub, format_num]),
                        'profit': compose([to_rub, format_num]),
                    })

                    db.save_row('ms_profitability', to_save_db)

                tg.send_message(f'saved data count: {len(profits)}')

            tg.send_message('end')
    except Exception as err:
        tg.error_traceback()
    finally:
        db.destroy()
