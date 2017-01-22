#!/usr/bin/env python3
# -*- coding:utf-8 -*- 

'''
collect all stock in shanghai/shenzhen A market
'''

import tushare as ts
import stock_orm, stock_base
import time
import logging
logger = logging.getLogger(__name__)

def get_stock_k_data(stock):
    df = ts.get_k_data(stock, start='2010-01-01',end='2016-12-31',ktype='D')
    df = df.set_index('date')
    return df

def offline_write_history_data():
    orm = stock_orm.StockOrmHK()
    stock_list = stock_base.get_stock_code()
    for code in stock_list:
        try:
            df = get_stock_k_data(code)
            orm.write_history_data(df, code)
            time.sleep(0.3)
        except Exception as e:
            logger.warning('{} has exception {}'.format(code, e))

if __name__ == '__main__':
    logging.basicConfig(filename='stock.log', format='%(asctime)s %(message)s', level=logging.INFO)
    stock_base.get_stock_code()
    #offline_write_history_data()


