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
company_name = "WoHongXiang"
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

### Tables in BG_DB
seller_table = 'vw_company_email_seller'
seller_product = 'tb_product_info_us'

### Tables in Currency_DB
currency_table = 'exchange_rate_daily'
