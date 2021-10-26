import streamlit as st

import altair as alt


# import plotly.express as px

import numpy as np
import pandas as pd
import datetime as dt
import time
import calendar
from datetime import date, timedelta
import xlrd
import openpyxl

# import dask
# import dask.dataframe as dd
#
# import config
# import simplejson
# import os
#
# from bs4 import BeautifulSoup
# import requests
# import re
#
# from zipfile import ZipFile, BadZipfile, is_zipfile
# from io import BytesIO

from streamlit import caching



st.set_page_config('Pricing data',layout='wide')


st.title('Australia electricity spot price history')

url = 'https://nemweb.com.au/Data_Archive/Wholesale_Electricity/MMSDM/2021/MMSDM_2021_09/MMSDM_Historical_Data_SQLLoader/DATA/PUBLIC_DVD_TRADINGPRICE_202109010000.zip'

path = 'C:/Users/matth/Documents/pythonprograms/Electricity_Prices/prices/'
#
# years = range(2009,2022)
# years = [str(year) for year in years]
# months = ['01','02','03','04','05','06','07','08','09','10','11','12']
#
# errors = []
#
# nbr = len(years)*len(months)
#
# i=1
# placeholder = st.empty()
# progholder = st.empty()
# mybar = st.progress(0)
#
# for year in years:
#     for month in months:
#         url = 'https://nemweb.com.au/Data_Archive/Wholesale_Electricity/MMSDM/'+year+'/MMSDM_'+year+'_'+month+'/MMSDM_Historical_Data_SQLLoader/DATA/PUBLIC_DVD_TRADINGPRICE_'+year+month+'010000.zip'
#         filename = 'PUBLIC_DVD_TRADINGPRICE_'+year+month+'010000.zip'
#
#         with open(path+filename,'wb') as zip:
#             try:
#                 r= requests.get(url)
#                 zip.write(r.content)
#             except:
#                 errors.append(filename)
#
#         with placeholder:
#             st.write('File #{0} complete '.format(i)+'/ '+str(nbr)+'.')
#         with progholder:
#             pct_complete = '{:,.2%}'.format(i/nbr)
#             st.write(pct_complete,' complete.' )
#             mybar.progress(i/nbr)
#         i=i+1
#
# st.write(errors)

# downloadlist = os.listdir(path)
# badlist = []
# for f in downloadlist:
#     if is_zipfile(path+f) == False:
#         badlist.append(f)
#
# computelist = list(set(downloadlist)-set(badlist))
#
# parts = [dask.delayed(pd.read_csv)(path+f,usecols=range(0,9),skiprows=1,header=0,parse_dates=['SETTLEMENTDATE']) for f in computelist]
# df = dd.from_delayed(parts)
# df = df[df['PRICE']=='PRICE']
# df = df[['SETTLEMENTDATE','REGIONID','RRP']]
# df.compute().to_csv(path+'prices_data.csv',mode='a')
#

@st.cache(suppress_st_warning=True)
def getpricingdata():
    # prices = pd.read_csv(path+'prices_data.csv')
    prices = pd.read_csv('prices_data.zip')
    prices = prices[['SETTLEMENTDATE','REGIONID','RRP']]
    prices.columns = ['Time','State','Price']
    # prices.drop_duplicates(keep='first',inplace=True)
    prices['Price'] = pd.to_numeric(prices['Price'],errors='coerce')
    # prices = prices[prices['Price']!=0]
    prices['Time'] = pd.to_datetime(prices['Time'],errors='coerce')
    prices = prices.sort_values('Time',ascending=False)
    prices['Year'] = pd.DataFrame(prices['Time']).apply(lambda x: x.dt.year)
    return prices


prices = getpricingdata()

st.markdown('Data source: '+'https://www.aemo.com.au/energy-systems/electricity/national-electricity-market-nem/data-nem/market-data-nemweb')

period = st.slider('Select period',2009,2021,(2009,2021),1)


prices = prices[(prices['Year']>=float(period[0]))&(prices['Year']<=float(period[1]))]


###### Filter prices ######

regionlist = ['QLD1','SA1','TAS1','NSW1','VIC1']

region = st.selectbox('Select State',regionlist,0)

regionfilter = prices[prices['State']==region]

highlight = alt.selection(type='interval',bind='scales',encodings=['x','y'])

fig = alt.Chart(regionfilter).mark_bar().encode(alt.X('Price:Q',scale=alt.Scale(domain=(-100,200)),bin=alt.BinParams(maxbins=5000)),alt.Y('count()'),color='State:N',tooltip=[
      {"type": "quantitative", "field": "Price"},
      {"type": "nominal", "field": "State"}]).add_selection(highlight)
st.altair_chart(fig,use_container_width=True)


###### End Filter prices ####

prices = prices.pivot_table(index='Time',columns='State',values='Price')



distrib = prices.describe([0.01,0.025,0.05,0.1,0.25,0.3,0.4,0.5,0.6,0.75,0.9,0.95,0.975,0.99])

st.write(distrib)


def convert_df(df):
    return df.to_csv().encode('utf-8')

csv = convert_df(distrib)

st.download_button(label = 'Download distribution as csv',data=csv,file_name='price_distribution.csv',mime='text/csv')
