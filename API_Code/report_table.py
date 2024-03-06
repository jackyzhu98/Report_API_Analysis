### 多店铺信息
import pandas as pd
import datetime
def Table_4(order_df, receive_df):

    monthly_data = pd.merge(order_df, receive_df, on = ['seller_id','currency_code','year','month'],how = 'outer')
    monthly_data.index = ['seller_id','currency_code','year', 'month', '销量', '销售额', '回款','订单数量']
    df_2023 = monthly_data[monthly_data['year'] == 2023]
    yearly_data = df_2023.groupby(['seller_id','year'])['销售额','回款'].sum().reset_index() 
    yearly_data[['销售额','回款']] = yearly_data[['销售额','回款']].div(10000).round(2)
    yearly_order = yearly_data.sort_values(by = ['销售额'],ascending = False)
    return yearly_order

def Table_5(order_df,receive_df):
    ## Table_5.1
    monthly_data = pd.merge(order_df, receive_df, on = ['seller_id','currency_code','year','month'],how = 'outer')
    monthly_data.index = ['seller_id','currency_code','year', 'month', '销量', '销售额', '回款','订单数量']
    df_2023 = monthly_data[((monthly_data['year']==2023)&(monthly_data['month']>1))|((monthly_data['year']==2024)&(monthly_data['month']==1))] 
    df_2023['date'] = pd.to_datetime(df_2023['year'].astype(str) + '-' + df_2023['month'].astype(str)).dt.strftime('%Y-%m')
    res = df_2023.groupby(['date'])['销售额','回款'].sum().round().astype(int).reset_index()
    
    ## Top 10 店铺

