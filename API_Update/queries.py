from config import *
import pandas as pd



finance_filter = f"""fund_transfer_status = 'Succeeded'""" #成功收到回款
###############
#####货币汇率###
###############

cur_query =f""" 
            SELECT 
                currency_code,exchange_rate,year,month,day 
            FROM 
                analyze_support_data.exchange_rate_daily 
            WHERE 
                year > 2021
            """ ## 大概率只需要2022年后的汇率
cur_cols = ['currency_code','rate','year','month','day']

###############
###店铺-订单记录#
###############

order_filter = f""" amount <> '' and order_status = 'Shipped' """ #数额大于0且成功送达
seller_order_qurcols =  ['_seller_id', 'currency_code', 'amount','number_of_items_shipped','sales_channel','amazon_order_id','Year(purchase_date)','Month(purchase_date)','Day(purchase_date)']

seller_order_query = f"""
            SELECT 
                {', '.join(seller_order_qurcols)}
            FROM 
                {'.'.join([api_db,order_table])}
            WHERE 
                {seller_filter} and {order_filter}
                        """
seller_order_cols =  ['seller_id', 'currency_code', 'amount','quantity_shipped','marketplace','amazon_order_id','year','month','day']

###############
###店铺-产品记录##
###############
seller_inventory_query = f"""
                    SELECT 
                        _seller_id,seller_sku,asin
                    FROM 
                        tb_fba_inventory_v1_inventorysummaries
                    WHERE 
                        {seller_filter}"""
seller_inventory_cols = ['seller_id','seller_sku','asin']

###############
###订单-产品信息#
###############

order_product_cols =  ['seller_id','amazon_order_id','marketplace','currency_code','year','month','day','seller_sku','quantity_shipped','charge_list','fee_list','tax_list']
order_product_query = f"""
            SELECT 
                a._seller_id,
                a.amazon_order_id,
                a.sales_channel,
                a.currency_code,
                YEAR(a.purchase_date) AS year,
                MONTH(a.purchase_date) AS month,
                DAY(a.purchase_date) AS day,
                c.seller_s_k_u,
                c.quantity_shipped,
                c.item_charge_list,
                c.item_fee_list,
                c.item_tax_withheld_list
            FROM
                (SELECT 
                    _seller_id,
                    amazon_order_id,
                    currency_code,
                    sales_channel,
                    purchase_date
                FROM 
                    {api_db}.{order_table}
                WHERE 
                    {seller_filter} AND {order_filter}) a
            LEFT JOIN
                {api_db}.{order_events} b
            ON 
                a.amazon_order_id = b.amazon_order_id
            LEFT JOIN
                (SELECT 
                    _root_id,
                    seller_s_k_u,
                    quantity_shipped,
                    item_charge_list,
                    item_fee_list,
                    item_tax_withheld_list
                FROM 
                    {api_db}.{order_details}) c
            ON 
                b.id = c._root_id;
            """

###############
###退单记录#####
###############

charge_back_cols = ['seller_id','amazon_order_id','marketplace','currency_code','seller_sku','quantity_shipped','charge_adj_list','fee_adj_list','tax_adj_list','year','month','day']
charge_back_query = f"""
            SELECT 
                a._seller_id,
                b.amazon_order_id, 
                b.marketplace_name,
                c.currency_code,
                a.seller_s_k_u,
                a.quantity_shipped,
                a.item_charge_adjustment_list,
                a.item_fee_adjustment_list,
                a.item_tax_withheld_list,
                c.year,
                c.month,
                c.day
            FROM
                (SELECT 
                    _root_id,
                    _seller_id,
                    seller_s_k_u,
                    quantity_shipped,
                    item_charge_adjustment_list,
                    item_fee_adjustment_list,
                    item_tax_withheld_list 
                FROM 
                    {'.'.join([api_db,refund_details])} 
                WHERE 
                    _parent_table = '{refund_table}' and {seller_filter}) a
                
            INNER JOIN
                (SELECT 
                    id, 
                    amazon_order_id, 
                    marketplace_name
                FROM  
                    {'.'.join([api_db,refund_table])}
                WHERE 
                    {seller_filter}) b
            ON 
                a. _root_id = b.id
            LEFT JOIN
                (SELECT 
                    distinct amazon_order_id,
                    currency_code, 
                    year(purchase_date) year, 
                    month(purchase_date) month, 
                    day(purchase_date) day
                FROM 
                    tb_orders_v0_orders_orders
                WHERE 
                    {seller_filter}) c
            ON b.amazon_order_id = c.amazon_order_id
"""
###############
###回款记录#####
###############
finance_qurcols = ['_seller_id' , 'original_total__currency_code', 'original_total__currency_amount','fund_transfer_date date' ,'Year(fund_transfer_date)','Month(fund_transfer_date)','Day(fund_transfer_date)']
finance_filter = f"""fund_transfer_status = 'Succeeded'""" #成功收到回款

finance_query = f"""
            SELECT 
                {', '.join(finance_qurcols)}
            FROM 
                {'.'.join([api_db,finance_table])}
            WHERE 
                {seller_filter} and {finance_filter}
            """
finance_cols = ['seller_id','currency_code','amount','date','year','month','day']

###############
###入库记录#####
###############

inbound_qurcols = ['_seller_id','seller_s_k_u','quantity_received','_last_updated_after','Year(_last_updated_after)','Month(_last_updated_after)','Day(_last_updated_after)']

inbound_query = f"""
            SELECT {', '.join(inbound_qurcols)}
            FROM {'.'.join([api_db,inbound_table])}
            WHERE {seller_filter}
                        """
inbound_cols = ['seller_id','seller_sku','quantity_received','date','year','month','day']

##############
####订单取消率#
##############

cancel_query = f"""
            SELECT 
                YEAR(purchase_date) as Year,
                MONTH(purchase_date) as Month,
                sales_channel,
                COUNT(DISTINCT amazon_order_id) AS total_orders,
                COUNT(DISTINCT CASE
                        WHEN order_status = 'Shipped' THEN amazon_order_id
                    END) AS shipped_orders,
                (COUNT(DISTINCT CASE
                        WHEN order_status = 'Shipped' THEN amazon_order_id
                    END) / COUNT(DISTINCT amazon_order_id)) * 100 AS shipped_rate,
                COUNT(DISTINCT CASE
                        WHEN order_status = 'Canceled' THEN amazon_order_id
                    END) AS canceled_orders,
                (COUNT(DISTINCT CASE
                        WHEN order_status = 'Canceled' THEN amazon_order_id
                    END) / COUNT(DISTINCT amazon_order_id)) * 100 AS cancel_rate
            FROM
                {api_db}.{order_table}
            WHERE
                {seller_filter}
            GROUP BY YEAR(purchase_date) , MONTH(purchase_date) , sales_channel;"""
cancel_cols = ['year','month','market','total_orders','shipped_orders','shipped_rate','canceled_orders','canceled_rate']

raw_query_list = ['seller_order','seller_inventory','order_product','charge_back','finance','currency','inbound','cancel']
raw_query_dict = {'seller_order':seller_order_query,'seller_inventory':seller_inventory_query,'order_product':order_product_query,'charge_back':charge_back_query,'finance':finance_query,'currency':cur_query,'inbound':inbound_query,'cancel':cancel_query}
raw_col_dict = {'seller_order':seller_order_cols,'seller_inventory':seller_inventory_cols,'order_product':order_product_cols,'charge_back':charge_back_cols,'finance':finance_cols,'currency':cur_cols,'inbound':inbound_cols,'cancel':cancel_cols}



def query_func(connection,query,col_name):
    cursor = connection.cursor()
    cursor.execute(query)
    query_result = cursor.fetchall()
    query_result = pd.DataFrame(query_result)
    query_result.columns = col_name
    query_result = query_result.drop_duplicates()
    cursor.close()
    return query_result


def basic_data_query(connection,raw_query_list):
    for i in raw_query_list:
        qur =  raw_query_dict[i]
        col =  raw_col_dict[i]
        try:
            df = query_func(connection = connection, query= qur, col_name= col)
            #df = df.drop_duplicates()
            df.to_csv(i+'.csv')
        except:
            print("Query Failed: " + i)

