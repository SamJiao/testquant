#!/usr/bin/env python3
# -*- coding:utf-8 -*- 

import argparse
import dateutil
import tushare as ts
import stock_orm, stock_base

def del_row(category, date):
    if(category == 'HD'):
        orm = stock_orm.StockOrmHD()
    elif(category == 'HK'):
        orm = stock_orm.StockOrmHK()
    stock_list = stock_base.get_stock_code()
    for code in stock_list:
        orm.del_row(code, date)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("type", type=str, choices=['HD', 'HK'])
    parser.add_argument("date", type=str, nargs="?", default=ts.util.dateu.today())
    args = parser.parse_args()
    dateutil.parser.parse(args.date)
    del_row(args.type, args.date)
