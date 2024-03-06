from config import *
import pandas as pd 

## 店铺销售量数据
def order_finance(order_data,finance_data,cur_data):
    ## 汇率转化
    order_data = order_data.merge(cur_data,how = 'left',on = ['currency_code','year','month','day'])
    order_data['amount_usd'] = order_data['amount']/order_data['rate']
    ## 月度数据
    monthly_order_data = order_data.groupby(['seller_id','marketplace','currency_code','year','month']).agg({'amount_usd':'sum','quantity_shipped':'sum','amazon_order_id':'nunique'}).reset_index() 
    monthly_order_data.to_csv(company_name+'_月度店铺销量.csv')

    ## 店铺回款数据    
    finance_data = finance_data.merge(cur_data,how = 'left',on = ['currency_code','year','month','day'])
    finance_data['amount_usd'] = finance_data['amount']/finance_data['rate']
    monthly_finance_data = finance_data.groupby(['seller_id','year','month','currency_code'])[['amount_usd']].sum().reset_index()
    monthly_finance_data.to_csv(company_name+'_月度店铺回款.csv')

    monthly_data = pd.merge(monthly_order_data, monthly_finance_data, on = ['seller_id','currency_code','year','month'],how = 'outer')
    monthly_data.index = ['seller_id','marketplace','currency_code','year', 'month', '销量', '销售额', '回款','订单数量']
    monthly_data['date'] = pd.to_datetime(monthly_data['year'].astype(str) + '-' + monthly_data['month'].astype(str)).dt.strftime('%Y-%m') 

    monthly_data.to_csv('Monthly_Order_Finance.csv')



