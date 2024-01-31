import mysql.connector
from config import *
from queries import *
from aggregate_func import *
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
    basic_path = os.path.join(output_path,'Basic Data')
    agg_path = os.path.join(output_path,'Agg Data')

    if not os.path.exists(basic_path):
        os.makedirs(basic_path)

    os.chdir(basic_path)
    print("Querying Basic Data...")
    basic_data_query(connection,raw_query_list)
    print("Query Completed")

    ## Data Read
    
    order_data = pd.read_csv('seller_order.csv')
    finance_data = pd.read_csv('finance.csv')
    order_product = pd.read_csv('order_product.csv')
    inventory = pd.read_csv('seller_inventory.csv')
    currency_data = pd.read_csv('currency.csv')
    refund_data = pd.read_csv('charge_back.csv')
    
    if not os.path.exists(agg_path):
        os.makedirs(agg_path)
    os.chdir(os.path.join(output_path,'Agg Data'))
    print("开始店铺销售量分析...")
    seller_order(order_data,currency_data)
    print("店铺销售量分析结束...")

    print("开始店铺回款分析...")
    seller_finance(finance_data,currency_data)
    print("店铺回款分析结束...")

    print("开始扣款分析...")
    fee_analysis(order_product,inventory,currency_data)
    print("扣款分析结束...")

    print("开始退款分析...")
    refund_analysis(refund_data,order_data,inventory,currency_data)
    print("退款分析结束...")

    print("开始产品销售量分析...")
    product_sales(order_product,order_data,inventory,currency_data)
    print("产品销售量分析结束...")