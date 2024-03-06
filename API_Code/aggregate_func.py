
import json
import os
from config import *

## 店铺销售量数据
def seller_order(order_data,cur_data):
    order_data = order_data.merge(cur_data,how = 'left',on = ['currency_code','year','month','day'])
    order_data['amount_usd'] = order_data['amount']/order_data['rate']
    monthly_order_data = order_data.groupby(['seller_id','marketplace','year','month']).agg({'amount_usd':'sum','quantity_shipped':'sum','amazon_order_id':'nunique'}).reset_index()
    daily_order_data = order_data.groupby(['seller_id','marketplace','year','month','day']).agg({'amount_usd':'sum','quantity_shipped':'sum','amazon_order_id':'nunique'}).reset_index()
    monthly_order_data.to_csv(company_name+'_店铺销量.csv')
    daily_order_data.to_csv(company_name+'_每日店铺销量.csv')

## 店铺回款数据
def seller_finance(finance_data,cur_data):
    finance_data = finance_data.merge(cur_data,how = 'left',on = ['currency_code','year','month','day'])
    finance_data['amount_usd'] = finance_data['amount']/finance_data['rate']
    monthly_finance_data = finance_data.groupby(['seller_id','year','month','currency_code'])[['amount_usd']].sum().reset_index()
    daily_finance_data = finance_data.groupby(['seller_id','year','month','day','currency_code'])[['amount_usd']].sum().reset_index()
    monthly_finance_data.to_csv(company_name+'_店铺回款.csv')
    daily_finance_data.to_csv(company_name+'_每日店铺回款.csv')


## 扣款数据
def fee_analysis(order_product,inventory,cur_data):
    order_product = order_product.merge(inventory,how = 'left', on = ['seller_id','seller_sku'])
    order_product['asin'] = order_product['asin'].fillna('Missing')
    order_product = order_product.merge(cur_data, how = 'left', on = ['year','month','day','currency_code'])
    
    for ind,row in order_product.iterrows():
        try:
            charge_list = json.loads(row['charge_list']) ## 扣除费用
            product_sales = charge_list[0]
            assert product_sales['ChargeType'] == 'Principal'
            
            order_product.loc[ind,'Principal'] = product_sales['ChargeAmount_CurrencyAmount']/row['rate']
        except:
            continue

    fee_type = []
    for ind,row in order_product.iterrows():
        try:
            fee_list = json.loads(row['fee_list']) ## 扣除费用
            for i in fee_list:
                fee_type.append(i['FeeType'])
                fee_type = list(set(fee_type))
                if i['FeeAmount_CurrencyCode'] != 'USD':
                    order_product.loc[ind,i['FeeType']] = i['FeeAmount_CurrencyAmount']/row['rate']
                else:
                    order_product.loc[ind,i['FeeType']] = i['FeeAmount_CurrencyAmount']
        except:
            continue


    tax_type = []
    for ind,row in order_product.iterrows():
        try:
            tax_list = json.loads(row['tax_list'])[0]
            order_product.loc[ind,'TaxCollectionModel'] = tax_list['TaxCollectionModel']
            tax_info = tax_list['TaxesWithheld']
            for i in tax_info:
                tax_type.append(i['ChargeType'])
                tax_type = list(set(tax_type))

                if i['ChargeAmount_CurrencyCode'] != 'USD':
                    order_product.loc[ind,i['ChargeType']] = i['ChargeAmount_CurrencyAmount'] / row['rate']
                else:
                    order_product.loc[ind,i['ChargeType']] = i['ChargeAmount_CurrencyAmount']
        except:
            continue

    order_product['year_month'] = order_product['year'].astype(int).astype(str) +'_' + order_product['month'].astype(int).astype(str)   
    result_df = order_product.groupby(['year','month','seller_id','marketplace','asin'])[['Principal'] + fee_type + tax_type+ ['quantity_shipped']].sum().reset_index()
    result_df['Total_Sales'] = result_df['Principal']
    result_df['Total_Fee'] = result_df[fee_type].sum(axis = 1)
    result_df['Total_Tax'] = result_df[tax_type].sum(axis = 1)
    #result_df  = result_df[['year_month','seller_id','marketplace','asin','quantity_shipped','Total_Sales','Total_Fee','Total_Tax']]
    result_df.to_csv(company_name+'_产品分析.csv')

## 退款分析
def refund_analysis(refund_data,order_data,inventory,cur_data):
    refund_data = refund_data.merge(inventory,how = 'left', on = ['seller_id','seller_sku'])
    refund_data.asin = refund_data.asin.fillna('Missing')
    refund_data = refund_data.drop_duplicates()
    refund_data = refund_data.merge(cur_data,how = 'inner', on = ['currency_code','year','month','day'])    
    charge_type = []
    fee_type = []
    for ind,row in refund_data.iterrows():
        ## Charge
        try:
            fee_list = json.loads(row['charge_adj_list'])
            for i in fee_list:
                charge_type.append(i['ChargeType'])
                if i['ChargeAmount_CurrencyCode'] != 'USD':
                    refund_data.loc[ind,i['ChargeType']] = i['ChargeAmount_CurrencyAmount'] / row['rate']
                else:
                    refund_data.loc[ind,i['ChargeType']] = i['ChargeAmount_CurrencyAmount']
        except:
            print('Charge_Error')
            print(row['charge_adj_list'])

        ## Fee
        try:
            fee_list_2 = json.loads(row['fee_adj_list'])
            for i in fee_list_2:
                fee_type.append(i['FeeType'])
                if i['FeeAmount_CurrencyCode'] != 'USD':
                    refund_data.loc[ind,i['FeeType']] = i['FeeAmount_CurrencyAmount'] / row['rate']
                else:
                    refund_data.loc[ind,i['FeeType']] = i['FeeAmount_CurrencyAmount']
        except:
            print('Fee_Error')
            print(row['fee_adj_list'])
    charge_type = list(set(charge_type))
    fee_type = list(set(fee_type))

    order_data['date'] = order_data['year'].astype(int).astype(str) + '-' + order_data['month'].astype(int).astype(str)
    refund_data['date'] = refund_data['year'].astype(int).astype(str) +'-' +refund_data['month'].astype(int).astype(str)
    result_df = refund_data.groupby(['date','seller_id','marketplace','amazon_order_id','asin'])[charge_type+fee_type+['quantity_shipped']].sum().reset_index()
    result_df['Total_Charge'] = result_df[charge_type].sum(axis = 1)
    result_df['Total_Fee'] = result_df[fee_type].sum(axis = 1)
    result_df['Total'] = result_df[charge_type+fee_type].sum(axis = 1)
    order_data = order_data.groupby(['seller_id','date','marketplace'])['amazon_order_id'].nunique().reset_index()
    order_data.columns = ['seller_id','date','marketplace','order_amount']
    result_df = result_df.groupby(['date','seller_id','marketplace']).agg({'Principal':'sum','quantity_shipped':'sum','amazon_order_id':'nunique'}).reset_index()
    result_df = result_df.merge(order_data[['date','seller_id','order_amount','marketplace']], how = 'left',on = ['year_month','seller_id','marketplace'])
    result_df.columns = ['date','seller_id','market','退货产品总价','退货产品数量','退单数量','订单总数']

    result_df.to_csv(company_name+'_退款分析.csv',encoding = 'utf-8-sig')


## 入库分析
def inbound_analysis(inbound_data,inventory,order_product):
    order_product = order_product.groupby(['seller_id','seller_sku','year','month'])['quantity_shipped'].sum().reset_index().drop_duplicates()
    inbound = inbound_data.groupby(['seller_id','seller_sku','year','month'])['quantity_received'].sum().reset_index()
    inbound = order_product[['seller_id','seller_sku','year','month','quantity_shipped']].merge(inbound,how = 'outer', on = ['seller_id','seller_sku','year','month'])
    inbound['quantity_received'] = inbound['quantity_received'] .fillna(0)
    inbound['quantity_shipped'] = inbound['quantity_shipped'] .fillna(0)
    inbound = inbound.sort_values(['seller_id','seller_sku','year','month'],ascending = True)
    inbound['inventory'] = inbound.groupby(['seller_sku'])['quantity_received'].cumsum() - inbound.groupby(['seller_sku'])['quantity_shipped'].cumsum()
    inbound = inbound.merge(inventory[['seller_id','seller_sku','asin']],how = 'left', on = ['seller_id','seller_sku'])
    inbound['inventory_filled'] = inbound['inventory'].apply(lambda x:0 if x<0 else x)
    inbound.to_csv(company_name+'_入库分析.csv',encoding = 'utf-8-sig')
