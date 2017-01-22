#!/usr/bin/env python3
# -*- coding:utf-8 -*- 

'''
collect all stock in shanghai/shenzhen A market for today
run this after close of the market
'''

import pandas as pd
import time
import logging
from logging.handlers import RotatingFileHandler
import argparse
import dateutil
import tushare as ts
import stock_orm, stock_base

formatter = logging.Formatter('%(asctime)s %(message)s')
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
handler = RotatingFileHandler("stock.log", maxBytes=1024*1024,backupCount=2)
handler.setFormatter(formatter)
logger.addHandler(handler)
def get_stock_today_data_hd(stock, start, end):
    df = ts.get_hist_data(stock, start=start,end=end,ktype='D')
    df = df.sort_index(ascending=True)
    return df

##def get_stock_today_data_hk_old(stock, start, end):
##    df = ts.get_k_data(stock, start=start,end=end,ktype='D')
##    df = df.set_index('date')
##    return df

def get_stock_today_data_hk(stock, start, end):
    df = ts.get_hist_data(stock, start=start,end=end,ktype='D')
    df1 = pd.DataFrame(df, columns=['open','close','high','low','volume'])
    df1['code']=stock
    df1 = df1.sort_index(ascending=True)
    return df1

def write_today_data(category,start, end):
    if(category == 'HD'):
        orm = stock_orm.StockOrmHD()
    elif(category == 'HK'):
        orm = stock_orm.StockOrmHK()
    stock_list = stock_base.get_stock_code()
    for code in stock_list:
        try:
            if(category == 'HD'):
                df = get_stock_today_data_hd(code, start, end)
            elif(category == 'HK'):
                df = get_stock_today_data_hk(code, start, end)
            orm.write_history_data(df, code)
            time.sleep(0.002)
        except Exception as e:
            logger.warning('{} has exception {}'.format(code, e))

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("type", type=str, choices=['HD', 'HK'])
    parser.add_argument("start", type=str, nargs="?", default=ts.util.dateu.today())
    parser.add_argument("end", type=str, nargs="?", default=ts.util.dateu.today())
    args = parser.parse_args()
    dateutil.parser.parse(args.start)
    dateutil.parser.parse(args.end)
    logger.info("write data for specific time windows: {}:{}:{}".format(args.type,args.start, args.end))
    write_today_data(args.type, args.start, args.end)


