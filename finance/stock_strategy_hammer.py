#!/usr/bin/env python3
# -*- coding:utf-8 -*- 

'''
Strategy candlestick pattern: hammer
'''
import logging, time, datetime, dateutil, argparse
from logging.handlers import RotatingFileHandler
import stock_orm, stock_base

formatter = logging.Formatter('%(asctime)s %(message)s')
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
handler = RotatingFileHandler("stock.log", maxBytes=1024*1024,backupCount=2)
handler.setFormatter(formatter)
logger.addHandler(handler)
class Hammer(object):
    def __init__(self,):
        self.left_shift = 30   #5Days window
        self.right_shift = 2   #5Days window
        self.upperlimit = 0.2
        self.stemlimit = 0.4
        self.shortcut = 0.01 #if it's a 光头

    def isahammer(self, openp, closep, highp, lowp):
        if openp >= closep:
            openp, closep = closep, openp
        if(openp == lowp):
            return False
        upperlimit = self.upperlimit
        stemlimit = self.stemlimit
        if((highp-closep)/closep < self.shortcut):
            stemlimit = 1
        upperper = (highp-closep)/(openp-lowp)
        stemper = (closep-openp)/(openp-lowp)
        if upperper <= upperlimit and stemper <= stemlimit:
            #logging.info('is hammer pattern')
            return True
        else:
            #logging.info('isn\'t hammer pattern')
            return False
    def isleftlowest(self, df):
        n=len(df)
        df1 = df.reset_index(drop=True)
        if df1.low.idxmin() == n-1:
            #logging.info('lastest is lowest')
            return True
        else:
            #logging.info('lastest is not lowest')
            return False

    def isrightlowest(self,df):
        df1 = df.reset_index(drop=True)
        if df1.low.idxmin() == 0 \
           and df1.high.idxmin() == 0:
            return True
        else:
            return False

    def ishammerpattern(self, df):
        fired_date=[]
        for n in range(self.left_shift, len(df) - self.right_shift+2):
            df1 = df[n-self.left_shift:n]
            df2= df[n-1:n+self.right_shift-1]
            #series = df1.loc[n-1]
            series = df.loc[n-1]
            #series_buy = df.loc[n+1]
            if self.isleftlowest(df1) \
               and self.isrightlowest(df2) \
               and self.isahammer(series.open, series.close,series.high, series.low):
                #logging.info('found')
                #print(n, series.date)
                fired_date.append(series.date)
        return fired_date


if __name__ == '__main__':
#    db = stock_orm.StockOrmHK()
#    codes=stock_base.get_stock_code()
#    dbquant = stock_orm.StockOrmQuantFire()
#    hammer=Hammer()
#    codes=['603999',]
#    for code in codes:
#        df = db.read_db_K_data(code)
#        logger.info(str(df[0:50]))
#        logger.info(str(df[50:100]))
#        logger.info(str(df[100:150]))
#        logger.info(str(df[150:200]))
#        logger.info(str(df[200:]))
#        datelist=hammer.ishammerpattern(df)
#        dbquant.append_fire_date(code, datelist)
#        print(datelist)
#        time.sleep(0.002)
    parser = argparse.ArgumentParser()
    parser.add_argument("date", type=str, nargs="?", default=str(datetime.datetime.today().date()))
    args = parser.parse_args()
    dateutil.parser.parse(args.date)
    logger.info("strategy hammer for date: {} begin".format(args.date))
    db = stock_orm.StockOrmHK()
    codes=stock_base.get_stock_code()
    dbquant = stock_orm.StockOrmQuantFire()
    hammer=Hammer()
    codetofile=[]
    for code in codes:
        df = db.read_db_K_data(code)
        try:
            df.sort_values(by='date',ascending=True,inplace=True)
            df.reset_index(drop=True, inplace=True)
            index = df[df.date==args.date].index[0]
            df=df[index-hammer.left_shift+1:index+hammer.right_shift].reset_index(drop=True)
            datelist=hammer.ishammerpattern(df)
            if not datelist:
                continue
            codetofile.append(code)
            dbquant.append_fire_date(code, datelist)
            time.sleep(0.002)
        except Exception as e:
            pass

    if codetofile:
        logger.info("find fired code")
        filename='data/{}.txt'.format(args.date)
        with open(filename, 'w+') as f:
            f.write('\n'.join(str(code) for code in codetofile))
    logger.info("strategy hammer for date: {} end".format(args.date))
