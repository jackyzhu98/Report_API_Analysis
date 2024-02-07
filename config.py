#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Jan 30 2024
@author: Jiaqi Zhu

"""
import os

###############
#需修改Configs
###############
path = "D:\合作店铺"
company_name = "中世"
# 1 - 松华 2 - 球德 3 - 沃鸿翔
company_ID = 3

db_user= 'jacky'
password= 'jacky_1017'
##########
#输出路径##
##########

output_path = os.path.join(path,company_name)

if not os.path.exists(output_path):
    os.makedirs(output_path)

###########
#数据库参数#
###########

## Connection
db_host = '192.168.2.108'


## Database
api_db = 'amazon_swagger'
background_db = 'zonglink_amazon_v9'
currency_db = 'analyze_support_data'

## Tables

### Tables in API_DB
order_table = 'tb_orders_v0_orders_orders'
order_events = 'tb_finances_v0_financialevents_shipmenteventlist'
order_details = 'tb_finances_v0_financialevents_shipmentitemlist'
finance_table =  'tb_finances_v0_financialeventgroups_financialeventgrouplist'
refund_table  = 'tb_finances_v0_financialevents_chargebackeventlist'
refund_details = 'tb_finances_v0_financialevents_shipmentitemadjustmentlist'
inv_table = 'tb_fba_inventory_v1_inventorysummaries'
inbound_table = 'tb_fba_inbound_v0_shipmentitems_itemdata'


### Tables in BG_DB
seller_table = 'vw_company_email_seller'
seller_product = 'tb_product_info_us'

### Tables in Currency_DB
currency_table = 'exchange_rate_daily'


### Seller 筛选条件
seller_list = 1 ## 1-使用单独seller list 2-从表里取sellerlist
if seller_list == 1:
    seller_filter = f""" _seller_id in 
    ('A2UVSPW9T36W11', 'A3A2SKWMOC62VF', 'A2ST06RBRNM2YB', 'AYBG8KY93WXSD', 'A4UEEFW5C73SF')"""
else:
    seller_filter = f"""_seller_id collate utf8mb4_unicode_520_ci in 
                 (SELECT seller_code FROM {background_db}.vw_company_email_seller
                where company_id = {company_ID})"""