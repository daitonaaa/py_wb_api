import gspread
import gspread_dataframe as gd
import pandas as pd
from sqlalchemy import create_engine, text
from datetime import datetime, timedelta
from urllib.parse import quote
import requests as rq

def send_telegram_message(text_message):
    chat_id = -4020643298
    tg_token = '6236028980:AAHSNJaZQ3wFmtiviAvK26ABycfpG9vQz40'
    message = quote(f'Unit_economics 📊📊📊: {text_message}')

    rq.request(
        method='GET',
        url=f'https://api.telegram.org/bot{tg_token}/sendMessage?chat_id={chat_id}&text={message}'
    )

today_minus_30_days = datetime.now() - timedelta(days=30)
formatted_date = today_minus_30_days.strftime('%Y-%m-%d')

sh_auth_dict = {
    "type": "service_account",
    "project_id": "instreet-399812",
    "private_key_id": "056e00420b4e9d1521cdb0329f6dbad5b6bffb23",
    "private_key": "-----BEGIN PRIVATE KEY-----\nMIIEvgIBADANBgkqhkiG9w0BAQEFAASCBKgwggSkAgEAAoIBAQC+pDx+Ovt8ecRn\nR22StCg2b/VokvVDUnxdSjZ++OSBLEJylvFYnGD26uLele3bI8d81ybSC8iMLntv\nNq+xn3yfa/b1ISMJtI4ejmWdkp5NnGVkJYMJfpSL/1lvYPMrLRP7fk4mwnwNybUE\nSL0l9KKZGpEa2dv2ZG1Y0QlsSKvxvM7/hJi/pT3w4Tf8EeKuShJ1c4NZJcl+rOq+\nf+6CNutyGN7KvpTcMIDGd3MaWOhRPrq4g7093xdDMz2b3D+5nJxH5CSwY+UVV6BH\nrW5tg9dB1BAIGog+KlxHOYzbIimKTsus3m778zBA/GmOpVO8L+N92sA59PxjDvDn\nFzS+86NTAgMBAAECggEAVfqSQcfuJexw9LdVJqXTwQFrZ7dnn+4yooxW2Zr8y3kf\nmriSrokQfivfF838kSmozZfnLTIdR4OrLbQ3Nt+b//ZcUZ+ZQsZIlmVYVOGMPap8\nW9dDKuQIsL8AIehSozVYXsNPEdICyuEen4kCrXyQXIaLzNVWi+AtJLJaEpkP2xyp\n3ntYPpUxSPMHFanlZC6YP4XLXFEWJ7MBU1bxdlXk70MVk4tsZBa60cOdD43h1t65\n7AcoFyBKk1hP3+toNkhzvf9ZJSKSC4uTn2UfRzUe5da3lj/wnhCHD9UCI/3DLMlz\n21XpxtQ9kuHIlkT9Z/on5fa3C6sKupioBbX/23O7MQKBgQDguTXmqxxqga2bqZXn\n/UwGCl8r/eRMaVqUlf2jiKamcRcsIHH8P67dXveDzCtW/UOt9TIIJ7cNYsFdQeRb\nvAccc1dNOISvlYNxCFrdlkHjPpBW36uYPokEHoBljZE1CAqniGI3gv50NAg1EIuS\nuerGyzu+YvTbJMh9bUQ8upHaCwKBgQDZLLP02atBZWllhngbco2+RpA3FXFChsy4\nM0xr9oveB3n142Tfxrhekbgw5Tr4zBkvH8thGtSFWrmdjdRPIfbMpJDv7GKf0J/D\n7SEw4VX6HoiPHdp5+wemVQ0DBWbViBKKmRoeXaWgyLNI+davJlJh1tzxjax8IymG\nn2etXudw2QKBgBfWjckyTYleyDPDAYviarYZU48vF5CjfGBWqNk4HbV6OaMMrgq8\nFqiU8lygw9gudYd2gyAqVlitKSXjX337rCVwdspSPIEDszXCVSE8VzDr6hPNFj7I\nI3C0a5paMhUWDUtIRGLvGl7pRhWa87NU1XrRBD1l8eNtO3dSp/JpOB+RAoGBAIOY\nsvMLctqNuO/sK5t2Lq291GWMeLT3OdXkP8qr+tTvctesktOGdvHXGuWGAdYA1sHX\nYkXVHDIyZ4cxChVpX0Nh9PFtK9XrdOQkLJiR8qc9GUKftGN12YzQvLG39Dgv3Axf\n+ZOs61AiTYFK4uv/LKxcpkU4s+wE4oTQ0uIbP+MBAoGBAK8ICSmbt+5zsobplTyS\nHLKC+23B/kigZLVjts/svvJBvkZyDAchhetV3s0LB8wunpKOt51eYc+eOHxRIhgd\noUoOIozbQa63AAYR7bcZI4RNXSD40itQDnMzatbA7vly0HupJVJX/5SVmCfBRIvK\n3/NvKnwaJDbJFyDKDhDoNNIj\n-----END PRIVATE KEY-----\n",
    "client_email": "instreet@instreet-399812.iam.gserviceaccount.com",
    "client_id": "115090958030512010782",
    "auth_uri": "https://accounts.google.com/o/oauth2/auth",
    "token_uri": "https://oauth2.googleapis.com/token",
    "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
    "client_x509_cert_url": "https://www.googleapis.com/robot/v1/metadata/x509/instreet%40instreet-399812.iam.gserviceaccount.com",
    "universe_domain": "googleapis.com"
}


if __name__ == '__main__':
    send_telegram_message('start')
    # In[2]:

    ### ВОТ ЭТО 2/4

    engine = create_engine('mysql+mysqlconnector://user205:b627e57c1b3bcfb7e4eb15be5be19019@db.retrafic.ru/user205')

    try:
        with engine.connect() as connection:
            send_telegram_message('step 1')
            query = text(f'''

    select

    qwerty_2.seller
    ,dated_list
    ,qwerty_2.supplierArticle
    ,qwerty_2.warehouseName
    ,sum(skl_1.quantity) as quantity

    from
        (
        select
        dated_list
        ,seller
        ,supplierArticle
        ,warehouseName
        ,techSize
        ,max(last_date) as last_date

        from
        (
        select
        d.dated_list
        ,skl.seller
        ,skl.supplierArticle
        ,skl.techSize
        ,skl.warehouseName
        ,quantity
        ,max(skl.last_date) as last_date

        from
        (
        select
        DATE_FORMAT(date,'%Y-%m-%d') as dated_list
        ,count(*)
        from user205.ipm_orders as da
        where date > '{formatted_date}'
        group by 1
        order by 1 DESC
        ) as d

        left join
        (
        select
        seller
        ,supplierArticle
        ,techSize
        ,warehouseName
        ,DATE_FORMAT(lastChangeDate,'%Y-%m-%d') as last_date
        ,round(sum(quantity)/count(quantity),0) as quantity
        from user205.warehouses_all
        where quantity != 0
        #and supplierArticle = '0130black'
        and warehouseName not in ('Санкт-Петербург','Екатеринбург','Подольск 3','Екатеринбург 2','Санкт-Петербург Шушары')
        group by 1,2,3,4,5
        ) as skl on d.dated_list >= skl.last_date

        group by 1,2,3,4,5,6
        ) as qwerty

        group by 1,2,3,4,5
        ) as qwerty_2

        left join
        (
        select
        seller
        ,supplierArticle
        ,techSize
        ,warehouseName
        ,DATE_FORMAT(lastChangeDate,'%Y-%m-%d') as last_date
        ,round(sum(quantity)/count(quantity),0) as quantity
        from user205.warehouses_all

        where quantity > 1
        and warehouseName not in ('Санкт-Петербург','Екатеринбург','Подольск 3','Екатеринбург 2','Санкт-Петербург Шушары')
        group by 1,2,3,4,5
        ) as skl_1

    on skl_1.supplierArticle = qwerty_2.supplierArticle
    and skl_1.techSize = qwerty_2.techSize
    and skl_1.warehouseName = qwerty_2.warehouseName
    and skl_1.last_date = qwerty_2.last_date
    and skl_1.seller = qwerty_2.seller

    where qwerty_2.dated_list >= '{formatted_date}'
    and skl_1.quantity is not null

    group by 1,2,3,4
    order by 2 desc
                          ''')

            result_1 = connection.execute(query)

            stock_df = pd.DataFrame(result_1.fetchall(), columns=result_1.keys())

            gc = gspread.service_account_from_dict(sh_auth_dict)
            sh = gc.open_by_key('1vdoXY88jnSHdP3HJ-5EXsTlJLdv4HjYFuuYV1ouIM3w')

            worksheet = sh.worksheet('warehouses')
            sh.values_clear("warehouses!C1:H1000000")
            gd.set_with_dataframe(worksheet, stock_df, 1, 3)

            ## step 2
            send_telegram_message('step 2')

            query = text(f'''

               SELECT
               globality.*
               ,ifnull(case when conversion.conv_ord_1 = 0 then conversion.conv_ord_2 else conversion.conv_ord_1 end,0.5) as conv_ord
               ,ifnull(case when conversion.ret_share_1 = 0 then conversion.ret_share_2 else conversion.ret_share_1 end,0.05) as ret_share
               ,seb_3.sebest as cogs_amount

               from

               (
               SELECT
               DATE_FORMAT(ipm_orders.`date`,'%Y-%m-%d') as date_ord
               ,ipm_orders.supplierArticle
               ,ipm_orders.nmId
               ,ifnull(round(sum(t5.avg_cost)/count(ipm_orders.priceWithDisc),0),0) as unit_logistics
               ,round(count(ipm_orders.priceWithDisc),2) as order_count
               ,round(sum(ipm_orders.priceWithDisc),2) as order_amount
               from user205.orders_all as ipm_orders
               left join user205.table_5 as t5 on t5.sa_name = ipm_orders.supplierArticle
               and t5.office_name = ipm_orders.warehouseName
               where ipm_orders.`date` > '{formatted_date}'
               group by 1,2
               order by 1 desc
               ) as globality
               left join (
               SELECT

               sa_name
               ,IFNULL(sum(case when bonus_type_name = 'К клиенту при продаже'
               and sale_dt > '2023-12-01' then 1 else 0 end)/
               (sum(case when bonus_type_name = 'К клиенту при отмене'
               and sale_dt > '2023-12-01' then 1 else 0 end)
               + sum(case when bonus_type_name = 'К клиенту при продаже'
               and sale_dt > '2023-12-01' then 1 else 0 end)),0) as conv_ord_1

               ,IFNULL(sum(case when bonus_type_name = 'От клиента при возврате'
               and sale_dt > '2023-12-01' then 1 else 0 end)/
               (sum(case when bonus_type_name = 'К клиенту при отмене'
               and sale_dt > '2023-12-01' then 1 else 0 end)
               + sum(case when bonus_type_name = 'К клиенту при продаже'
               and sale_dt > '2023-12-01' then 1 else 0 end)),0) as ret_share_1

               ,IFNULL(sum(case when bonus_type_name = 'К клиенту при продаже'
               and sale_dt BETWEEN '2023-11-01' and '2023-11-30' then 1 else 0 end)/
               (sum(case when bonus_type_name = 'К клиенту при отмене'
               and sale_dt BETWEEN '2023-11-01' and '2023-11-30' then 1 else 0 end)
               + sum(case when bonus_type_name = 'К клиенту при продаже'
               and sale_dt BETWEEN '2023-11-01' and '2023-11-30' then 1 else 0 end)),0.5) as conv_ord_2

               ,IFNULL(sum(case when bonus_type_name = 'От клиента при возврате'
               and sale_dt BETWEEN '2023-11-01' and '2023-11-30' then 1 else 0 end)/
               (sum(case when bonus_type_name = 'К клиенту при отмене'
               and sale_dt BETWEEN '2023-11-01' and '2023-11-30' then 1 else 0 end)
               + sum(case when bonus_type_name = 'К клиенту при продаже'
               and sale_dt BETWEEN '2023-11-01' and '2023-11-30' then 1 else 0 end)),0.05) as ret_share_2
               from user205.weekly_all
               group by 1 )
               as conversion
               on conversion.sa_name = globality.supplierArticle



               left JOIN (
               SELECT
               seb.art
               ,seb.sebest
               from user205.sebest as seb
               inner join
               (
               SELECT
               max(seb_1.date_start) as date_start
               from user205.sebest as seb_1
               ) as seb_2
               on seb.date_start = seb_2.date_start

               )
               as seb_3
               on seb_3.art = globality.supplierArticle


               union


               SELECT


               qwerty.date_ord
               ,qwerty.group_art as 'supplierArticle'
               ,"total"
               ,round(sum(qwerty.agg_logistics)/SUM(qwerty.order_count),2) as 'unit_logistics'
               ,SUM(qwerty.order_count) as 'order_count'
               ,round(sum(qwerty.order_amount),2) as 'order_amount'
               ,round(sum(qwerty.agg_purchased)/SUM(qwerty.order_count),4) as 'conv_ord'
               ,round(sum(qwerty.agg_returned)/SUM(qwerty.order_count),4) as 'ret_share'
               ,round(sum(qwerty.agg_cogs)/sum(qwerty.order_count),2) as 'cogs_amount'


               from (

               SELECT

               m.group_art
               ,globality.*

               ,ifnull(case when conversion.conv_ord_1 = 0 then conversion.conv_ord_2 else conversion.conv_ord_1 end,0.5) as conv_ord
               ,ifnull(case when conversion.ret_share_1 = 0 then conversion.ret_share_2 else conversion.ret_share_1 end,0.05) as ret_share
               ,seb_3.sebest as cogs_amount
               ,globality.order_count*globality.unit_logistics as agg_logistics
               ,round(globality.order_count*ifnull(case when conversion.conv_ord_1 = 0 then conversion.conv_ord_2 else conversion.conv_ord_1 end,0.5),2) as agg_purchased
               ,round(globality.order_count*ifnull(case when conversion.ret_share_1 = 0 then conversion.ret_share_2 else conversion.ret_share_1 end,0.05),2) as agg_returned
               ,round(globality.order_count*seb_3.sebest,2) as agg_cogs



               from

               (
               SELECT
               DATE_FORMAT(ipm_orders.`date`,'%Y-%m-%d') as date_ord
               ,ipm_orders.supplierArticle
               ,ipm_orders.nmId

               ,ifnull(round(sum(t5.avg_cost)/count(ipm_orders.priceWithDisc),0),0) as unit_logistics

               ,round(count(ipm_orders.priceWithDisc),2) as order_count
               ,round(sum(ipm_orders.priceWithDisc),2) as order_amount
               from user205.orders_all as ipm_orders
               left join user205.table_5 as t5 on t5.sa_name = ipm_orders.supplierArticle
               and t5.office_name = ipm_orders.warehouseName
               where ipm_orders.`date` > '{formatted_date}'

               group by 1,2
               order by 1 desc
               ) as globality
               left join (
               SELECT

               sa_name
               ,IFNULL(sum(case when bonus_type_name = 'К клиенту при продаже'
               and sale_dt > '2023-12-01' then 1 else 0 end)/
               (sum(case when bonus_type_name = 'К клиенту при отмене'
               and sale_dt > '2023-12-01' then 1 else 0 end)
               + sum(case when bonus_type_name = 'К клиенту при продаже'
               and sale_dt > '2023-12-01' then 1 else 0 end)),0) as conv_ord_1

               ,IFNULL(sum(case when bonus_type_name = 'От клиента при возврате'
               and sale_dt > '2023-12-01' then 1 else 0 end)/
               (sum(case when bonus_type_name = 'К клиенту при отмене'
               and sale_dt > '2023-12-01' then 1 else 0 end)
               + sum(case when bonus_type_name = 'К клиенту при продаже'
               and sale_dt > '2023-12-01' then 1 else 0 end)),0) as ret_share_1

               ,IFNULL(sum(case when bonus_type_name = 'К клиенту при продаже'
               and sale_dt BETWEEN '2023-11-01' and '2023-11-30' then 1 else 0 end)/
               (sum(case when bonus_type_name = 'К клиенту при отмене'
               and sale_dt BETWEEN '2023-11-01' and '2023-11-30' then 1 else 0 end)
               + sum(case when bonus_type_name = 'К клиенту при продаже'
               and sale_dt BETWEEN '2023-11-01' and '2023-11-30' then 1 else 0 end)),0.5) as conv_ord_2

               ,IFNULL(sum(case when bonus_type_name = 'От клиента при возврате'
               and sale_dt BETWEEN '2023-11-01' and '2023-11-30' then 1 else 0 end)/
               (sum(case when bonus_type_name = 'К клиенту при отмене'
               and sale_dt BETWEEN '2023-11-01' and '2023-11-30' then 1 else 0 end)
               + sum(case when bonus_type_name = 'К клиенту при продаже'
               and sale_dt BETWEEN '2023-11-01' and '2023-11-30' then 1 else 0 end)),0.05) as ret_share_2
               from user205.weekly_all
               group by 1 )
               as conversion
               on conversion.sa_name = globality.supplierArticle



               left JOIN (
               SELECT
               seb.art
               ,seb.sebest
               from user205.sebest as seb
               inner join
               (
               SELECT
               max(seb_1.date_start) as date_start
               from user205.sebest as seb_1
               ) as seb_2
               on seb.date_start = seb_2.date_start

               )
               as seb_3
               on seb_3.art = globality.supplierArticle

               left JOIN mapping m on m.supplierArticle = globality.supplierArticle
               ) as qwerty


               group by 1,2,3
               order by 1 desc



                                    ''')
            result_2 = connection.execute(query)

            orders_df = pd.DataFrame(result_2.fetchall(), columns=result_2.keys())
            orders_df['conv_ord'] = orders_df['conv_ord'].astype(float)
            orders_df['ret_share'] = orders_df['ret_share'].astype(float)

            gc = gspread.service_account_from_dict(sh_auth_dict)
            sh = gc.open_by_key('1vdoXY88jnSHdP3HJ-5EXsTlJLdv4HjYFuuYV1ouIM3w')

            worksheet = sh.worksheet('data')
            sh.values_clear("data!B1:J100000")
            gd.set_with_dataframe(worksheet, orders_df, 1, 2)

    finally:
        engine.dispose()



    # step 3
    send_telegram_message('step 3')
    engine = create_engine('mysql+mysqlconnector://user344:1d9dce46940f625466943eacc499718f@db.retrafic.ru/user344')

    try:
        with engine.connect() as connection:

            query = text(f'''
    SELECT 

    companyname
    ,date	
    ,upd_updsum	
    ,upd_advertid	
    ,day_apps_nm_views	
    ,day_apps_nm_clicks	
    ,day_apps_nm_ctr	
    ,day_apps_nm_cpc
    ,round(case when companyname in ('Пашкович','Акс','Банишевский') then day_apps_nm_sum/120*100 else day_apps_nm_sum end,2) as day_apps_nm_sum
    ,day_apps_nm_atbs	
    ,day_apps_nm_orders	
    ,day_apps_nm_cr	
    ,day_apps_nm_shks	
    ,day_apps_nm_nmid

    FROM user344.bronks 

    where user344.bronks.date > '{formatted_date}' 
    order by user344.bronks.date desc                

                         ''')

            result_1 = connection.execute(query)

    finally:

        engine.dispose()

    advert_df = pd.DataFrame(result_1.fetchall(), columns=result_1.keys())

    gc = gspread.service_account_from_dict(sh_auth_dict)
    sh = gc.open_by_key('1vdoXY88jnSHdP3HJ-5EXsTlJLdv4HjYFuuYV1ouIM3w')

    worksheet = sh.worksheet('advert')
    sh.values_clear("advert!a1:n100000")
    gd.set_with_dataframe(worksheet, advert_df, 1, 1)


    send_telegram_message('end')

