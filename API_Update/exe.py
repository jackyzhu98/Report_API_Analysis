import mysql.connector
from config import *
from queries import *
from agg_func import *
from report_table import *

import os
import pandas as pd
connection = mysql.connector.connect(
            host= db_host,
            database= api_db,
            user= db_user,
            password= password
)


if __name__ == '__main__':
    ## Query Basic Data
    raw_path = os.path.join(output_path,'Raw Data')
    agg_path = os.path.join(output_path,'Agg Data')
    table_path = os.path.join(output_path,'Table Data')
    
    if not os.path.exists(raw_path):
        os.makedirs(raw_path)
        os.chdir(raw_path)
    else:
        os.chdir(raw_path)
    print("Querying Basic Data...")
    basic_data_query(connection,raw_query_list)
    print("Query Completed")

    ## 读取Raw Data
    order_data = pd.read_csv('seller_order.csv')
    finance_data = pd.read_csv('finance.csv')
    order_product = pd.read_csv('order_product.csv')
    inventory = pd.read_csv('seller_inventory.csv')
    currency_data = pd.read_csv('currency.csv')
    refund_data = pd.read_csv('charge_back.csv')
    inbound_data = pd.read_csv('inbound.csv')
    
    if not os.path.exists(agg_path):
        os.makedirs(agg_path)
        os.chdir(agg_path)
    else:
        os.chdir(agg_path)
    
    ##第一次聚合
    order_finance(order_data,finance_data,currency_data) ## 订单销量回款
    product_analysis(order_product,inventory,currency_data) ## 产品粒度分析
    refund_analysis(refund_data,order_data,inventory,currency_data) ## 退款分析
    inbound_analysis(inbound_data,inventory,order_product) ## 入库分析

    ##读取Agg Data
    monthly_data = pd.read_csv("Monthly_Order_Finance.csv")
    product_data = pd.read_csv('product_data.csv')
    refund_data = pd.read_csv('refund_data.csv')
    if not os.path.exists(table_path):
        os.makedirs(table_path)
        os.chdir(table_path)
    else:
        os.chdir(table_path)


    ##输出报告用表格
    
    Table_4(monthly_data)
    Table_5_1(monthly_data)
    Table_5_2(monthly_data)
    Table_5_4(monthly_data)
    #Table_5_6_1(product_data)
    Table_5_6_3(product_data)
    Table_5_7_1(product_data)
    Table_5_7_2(product_data)
    Table_5_8(refund_data) 
    Table_5_9()

