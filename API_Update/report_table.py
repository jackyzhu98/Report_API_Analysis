from config import *
import pandas as pd
from datetime import datetime, timedelta
from sklearn.preprocessing import LabelEncoder
import matplotlib.pyplot as plt
import seaborn as sns
import mysql.connector

def Table_4(monthly_data):
    ## 选定时间范围
    monthly_data_2023 = monthly_data[monthly_data['year'] == 2023]

    yearly_data = monthly_data_2023.groupby(['seller_id','year'])[['销售额','回款']].sum().reset_index() 
    yearly_data[['销售额','回款']] = yearly_data[['销售额','回款']].div(10000).round(2)

    yearly_data.to_csv('Table_4.csv')

def Table_5_1(monthly_data):
    ## 选定时间范围
    dates = [datetime.strptime(date, '%Y-%m') for date in monthly_data.date]
    max_date = max(dates)
    start_date = max_date - timedelta(days=365)
    selected_dates = [date.strftime('%Y-%m') for date in dates if start_date <= date <= max_date]
    latest_12_df = monthly_data[monthly_data['date'].isin(selected_dates)]     
    
    table_5_1 = latest_12_df.groupby(['year','marketplace'])[['销量','销售额','回款']].sum().round().astype(int).reset_index()
    table_5_3 = latest_12_df.groupby(['date','marketplace'])[['销量', '销售额', '回款']].sum().round().astype('Int64').reset_index() 
    table_5_3 = table_5_3.sort_values(by=['marketplace','date'], ascending=True) 

    table_5_1.to_csv('Table_5_1.csv')
    table_5_3.to_csv('Table_5_3.csv')

def Table_5_2(monthly_data):
    
    ## 近12个月的回款
    dates = [datetime.strptime(date, '%Y-%m') for date in monthly_data.date]
    max_date = max(dates)
    start_date = max_date - timedelta(days=365)
    selected_dates = [date.strftime('%Y-%m') for date in dates if start_date <= date <= max_date]
    latest_12_df = monthly_data[monthly_data['date'].isin(selected_dates)] 
    
    ## 选取近12个月总销售额前十店铺作为top10 
    top_10_ids = latest_12_df.groupby(['seller_id'])['销售额'].sum().nlargest(10).index

    grouped = latest_12_df.groupby(['seller_id','date'])[['销售额','回款']].sum().round().astype('Int64').reset_index() 
    grouped.sort_values(['seller_id','date'], inplace=True) 

    sales_matrix = pd.pivot_table(grouped, values='销售额', index='date', columns='seller_id') 
    sales_matrix = sales_matrix.fillna(0) 
    
    payment_matrix = pd.pivot_table(grouped, values='回款', index='date', columns='seller_id') 
    payment_matrix = payment_matrix.fillna(0) 

    top_10 = monthly_data[(monthly_data['seller_id'].isin(top_10_ids))].copy() 

    #按照top_10_ids 的顺序对数据排序 

    top_10 = top_10.sort_values(['seller_id','currency_code','year','month']) 
    top_10[['销量', '销售额', '回款', '订单数量']] = top_10[['销量', '销售额', '回款', '订单数量']].round().fillna(0).astype(int) 

    ## 对比2022 2023
    top_10_2223 = top_10[((top_10['year'] == 2022) | (top_10['year'] == 2023))] 
    top_10_2223 = top_10_2223.groupby(['seller_id', 'year'])[['销售额', '回款']].sum().round().astype('Int64').reset_index() 
    top_10_2223.sort_values(['year','seller_id'], inplace=True) 
    top_10_2223['销售额'] = top_10_2223['销售额'].div(10000).round(2) 
    top_10_2223['回款'] = top_10_2223['回款'].div(10000).round(2) 

    column_names = [] 

    for year in sorted(top_10_2223['year'].unique()): 

        column_names.extend([f'{year}销售额', f'{year}回款']) 

    top_10_matrix = pd.pivot_table(top_10_2223, values=['销售额', '回款'], index='seller_id', columns=['year'],observed=False) 
    top_10_matrix.columns = [f'{column[1]}{column[0]}' for column in top_10_matrix.columns] 
    top_10_matrix = top_10_matrix.reindex(columns=column_names).transpose() 

    ## Output
    sales_matrix.to_csv('Table_5_2_1.csv')
    payment_matrix.to_csv('Table_5_2_2.csv')
    top_10_matrix.to_csv('Table_5_2_3.csv')

def Table_5_4(monthly_data):

    ## 选取2023年总销售额前十店铺作为top10 
    dates = [datetime.strptime(date, '%Y-%m') for date in monthly_data.date]
    max_date = max(dates)
    start_date = max_date - timedelta(days=365)
    selected_dates = [date.strftime('%Y-%m') for date in dates if start_date <= date <= max_date]
    latest_12_df = monthly_data[monthly_data['date'].isin(selected_dates)] 
    top_10_ids = latest_12_df.groupby(['seller_id'])['销售额'].sum().nlargest(10).index  
    top_10 = monthly_data[(monthly_data['seller_id'].isin(top_10_ids))].copy() 
    top_10 = top_10.groupby(['seller_id','date'])[['销量','回款']].sum().reset_index()
    le = LabelEncoder() ## 给店铺编号

    top_10['seller_code'] = le.fit_transform(top_10['seller_id'])
    
    ## 销量相关性
    sales_df = top_10.pivot(columns='seller_code',values = '销量',index= 'date').reset_index(drop=True)
    sales_corr = sales_df.corr().round(2)
    plt.figure(figsize=(20,20))
    ax = sns.heatmap(sales_corr,cmap='Blues',annot= True,annot_kws={'size': 20})
    cbar = ax.collections[0].colorbar
    cbar.ax.tick_params(labelsize=20)
    plt.xticks(fontsize = 20)
    plt.xlabel('Seller_Code', fontsize=14)
    plt.yticks(fontsize = 20)
    plt.ylabel('Seller_Code', fontsize=14)
    
    ###Output
    plt.savefig('Table_5_4_销量相关性.jpg',dpi = 300)
    #plt.show()
    ##回款相关性
    
    rec_df = top_10.pivot(columns='seller_code',values = '回款',index= 'date').reset_index(drop=True)
    rec_corr = rec_df.corr().round(2)
    plt.figure(figsize=(20,20))
    ax = sns.heatmap(rec_corr,cmap='Blues',annot= True,annot_kws={'size': 20})
    cbar = ax.collections[0].colorbar
    cbar.ax.tick_params(labelsize=20)
    plt.xticks(fontsize = 20)
    plt.xlabel('Seller_Code', fontsize=14)
    plt.yticks(fontsize = 20)
    plt.ylabel('Seller_Code', fontsize=14)

    ### Output
    plt.savefig('Table_5_4_回款相关性.jpg',dpi = 300)
    #plt.show()


def query_category_data(asin_list):
    connection = mysql.connector.connect(
            host= db_host,
            database= api_db,
            user= db_user,
            password= password
)

    ### 从US库里取
    cursor = connection.cursor()
    query = f"""
    SELECT asin,category_root,category_leaf
    FROM zonglink_amazon_v9.tb_product_info_us
    WHERE asin in ('{"', '".join(asin_list)}')"""
    cursor.execute(query)
    query_result = cursor.fetchall()
    query_result = pd.DataFrame(query_result)
    if(query_result.shape[1] == 3):
        query_result.columns = ['asin','category_root','category_leaf']
        query_result = query_result.drop_duplicates()
    cursor.close()
    
    ### 从EU库里取
    cursor = connection.cursor()
    query_eu = f"""
    SELECT asin,category_root,category_leaf
    FROM zonglink_amazon_v9.tb_product_info_eu
    WHERE asin in ('{"', '".join(asin_list)}')"""
    cursor.execute(query_eu)
    query_result_eu = cursor.fetchall()
    query_result_eu = pd.DataFrame(query_result_eu)
    if(query_result_eu.shape[1] == 3):
        query_result_eu.columns = ['asin','category_root','category_leaf']
        query_result_eu = query_result_eu.drop_duplicates()

    query_result = pd.concat([query_result,query_result_eu],axis = 0,ignore_index=True)
    query_result = query_result.drop_duplicates()
    return query_result

def Table_5_6_1(product_data):

    product_data = product_data[(product_data['asin'] != 'Missing')] 

    asin_df = product_data[['seller_id','asin']].drop_duplicates() 
    asin_list = asin_df.asin.unique().tolist()
    category_data = query_category_data(asin_list)
    asin_cat = asin_df.merge(category_data[['asin','category_root','category_leaf']],on = ['asin'],how = 'left') 
    category_counts = asin_cat['category_leaf'].value_counts() 
    
    ### 画图
    chinese_labels = []     # chinese_labels是根据'category_leaf'自行翻译的 
    colors = [] 
    small_categories = category_counts[category_counts / category_counts.sum() < 0.01] #根据图形种类多少自行设置，将比例过小的category归类为others，避免标签重叠
    small_categories_sum = small_categories.sum() 
    category_counts = category_counts[~category_counts.index.isin(small_categories.index)] 
    category_counts['其他'] = small_categories_sum 
    labels = chinese_labels 

    counts = category_counts.values.tolist() 

    plt.rcParams['font.family'] = 'Microsoft JhengHei'  
    fig, ax = plt.subplots(figsize=(12,10)) 
    _, _, autopcts = ax.pie(counts, labels=labels,colors = colors, autopct='%.2f%%', textprops={'fontsize': 13}) 

    for i in range (len(autopcts)):      
        if i<3:                                       #将产品主要类型的字体大小与其他类型区分开 
            autopcts[i].set_fontsize(12) 
        else: 
            autopcts[i].set_fontsize(8.5) 
    ax.set_aspect('equal') 
    
    ### Output
    #plt.savefig('')   
    plt.tight_layout()  
    plt.show() 

def Table_5_6_3(product_data): ###主要产品销售分析
    product_data['dates'] = pd.to_datetime(product_data['year'].astype(str) + '-' + product_data['month'].astype(str)).dt.strftime('%Y-%m')

    product_data = product_data[(product_data['asin'] != 'Missing')] 

    dates = [datetime.strptime(date, '%Y-%m') for date in product_data.dates]
    max_date = max(dates)
    start_date = max_date - timedelta(days=365)
    selected_dates = [date.strftime('%Y-%m') for date in dates if start_date <= date <= max_date]
    latest_12_df = product_data[product_data['dates'].isin(selected_dates)]
    ### Top 10 产品 
    top_10_asin = latest_12_df.groupby(['asin'])['Total_Sales'].sum().nlargest(10).index  
    top_10 = product_data[(product_data['asin'].isin(top_10_asin))].copy() 
    top_10 = top_10.sort_values(by='asin') 
    asin_df = top_10[['asin']].drop_duplicates()
    ### 上架日期
    earliest_dates = top_10.groupby('asin')['dates'].min() 
    top_10['earliest_date'] = top_10['asin'].map(earliest_dates) 
    earliest_dates_df = top_10[['asin','earliest_date']].drop_duplicates()    
    
    asin_df = pd.merge(asin_df, earliest_dates_df ,on = 'asin',how = 'left') 
    
    ### 2023 年销售额/年销售量/平均单价
    df1 = top_10[(top_10['year'] == '2023')] 
    sales_df = df1.groupby(['seller_id', 'asin']).agg({'Total_Sales': 'sum', 'quantity_shipped': 'sum'}).reset_index() 
    sales_df['Price'] = sales_df['Total_Sales']/sales_df['quantity_shipped']
    asin_df = pd.merge(asin_df, sales_df ,on = 'asin',how = 'left') 

    ### 成本 - 1688

    ### 类别l
    asin_list = top_10.asin.astype(str).unique().tolist()
    category_df = query_category_data(asin_list)
    table_5_6_3 = pd.merge(asin_df,category_df,on = 'asin',how = 'left')
    table_5_6_3.to_csv('Table_5_6_3.csv')

def Table_5_7_1(product_data):
    product_data['dates'] = pd.to_datetime(product_data['year'].astype(str) + '-' + product_data['month'].astype(str)).dt.strftime('%Y-%m')
    dates = [datetime.strptime(date, '%Y-%m') for date in product_data.dates]
    max_date = max(dates)
    start_date = max_date - timedelta(days=365)
    selected_dates = [date.strftime('%Y-%m') for date in dates if start_date <= date <= max_date]
    latest_12_df = product_data[product_data['dates'].isin(selected_dates)]

    table_5_7_1 = latest_12_df.groupby(['marketplace','dates'])[['Total_Fee','Total_Tax']].sum().reset_index()
    table_5_7_1['Total_Fee'] = table_5_7_1['Total_Fee'] + table_5_7_1['Total_Tax']
    table_5_7_1 = table_5_7_1.pivot(index='dates',columns='marketplace',values = 'Total_Fee').reset_index()
    table_5_7_1['Monthly_Total'] = table_5_7_1.iloc[:,1:].sum(axis = 1)
    market_total_row = table_5_7_1.iloc[:,1:].sum(axis = 0)
    table_5_7_1.loc[len(table_5_7_1)] = market_total_row
    table_5_7_1.to_csv('table_5_7_1.csv')


def Table_5_7_2(product_data):
    table_5_7_2 = product_data.groupby(['year'])[['Commission','FBAPerUnitFulfillmentFee','Total_Fee','Total_Tax']].sum().reset_index()
    table_5_7_2['Other_Fee'] = table_5_7_2['Total_Fee'] - table_5_7_2['Commission'] - table_5_7_2['FBAPerUnitFulfillmentFee']
    table_5_7_2 = table_5_7_2.drop(['Total_Fee'],axis = 1)
    table_5_7_2['Total'] = table_5_7_2.sum(axis = 1)
    table_5_7_2 = table_5_7_2.set_index('year')
    table_5_7_2 = table_5_7_2.transpose()
    table_5_7_2.to_csv('table_5_7_2.csv')
    
def Table_5_8(refund_data):
    table_5_8 = refund_data.groupby(['date','market'])[['退单数量','订单总数']].sum().reset_index()
    table_5_8['退貨率'] = table_5_8['退单数量']/table_5_8['订单总数']
    table_5_8 = table_5_8.pivot(index='date',values = ['订单总数','退貨率'],columns='market')
    table_5_8.to_csv('table_5_8.csv')

def Table_5_9():
    connection = mysql.connector.connect(
        host= db_host,
        database= api_db,
        user= db_user,
        password= password
    )
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
    cursor = connection.cursor()
    cursor.execute(cancel_query)
    table_5_9 = cursor.fetchall()
    table_5_9 = pd.DataFrame(table_5_9)
    table_5_9.columns = cancel_cols
    table_5_9 = table_5_9.drop_duplicates()
    cursor.close()

    table_5_9['date'] = table_5_9['year'].astype(str) + '-' +table_5_9['month'].astype(str)
    table_5_9 = table_5_9[['date','market','total_orders','shipped_orders','shipped_rate','canceled_orders','canceled_rate']]
    table_5_9.to_csv('table_5_9.csv')
    