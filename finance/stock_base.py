#!/usr/bin/env python3
# -*- coding:utf-8 -*- 

'''
collect all stock in shanghai/shenzhen A market
the data is due to 2016-12-26
'''

import tushare as ts
import stock_orm
import pandas as pd
import logging
logger = logging.getLogger(__name__)

class CodeList():
    usedb = True
    code_list = pd.Series()

def collect_stock_codes():
    df = ts.get_stock_basics()
    return df


def get_stock_code():
    logger.info('enter here')
    orm = stock_orm.StockOrm()
    if(CodeList.usedb == True):
        df = orm.read_stock_table()
        CodeList.code_list = df.code.copy().sort_values().reset_index(drop=True)
    else:
        pass
    return CodeList.code_list
