#!/usr/bin/env python3
# -*- coding:utf-8 -*- 

'''
mysql interface
'''

import json
import sqlalchemy
from sqlalchemy.types import VARCHAR
import pandas as pd
import logging
logger = logging.getLogger(__name__)

class StockOrm(object):
    '''Stock ORM manages mysql interface for finance data persistence'''
    engine = sqlalchemy.create_engine('mysql://root:password@localhost/stock?charset=utf8')
    def __init__(self, ):
        pass

    def exist(self, row, table):
        sql = 'select count(*) from `{}` where code="{}"'.format(table, row)
        logger.info('sql string is {}'.format(sql))
        result = self.engine.execute(sql).scalar()
        logger.info('result is: {}'.format(result))
        if result == 1:
            return True
        elif result == 0:
            return False
        elif result > 1 :
            logger.warning("duplicate record for ", row)
            return True

    def write_history_data(self, df, code):
        logger.info('{} write to database'.format(code))
        with self.engine.begin() as connection:
            #pd.io.sql.to_sql(df, code, connection, if_exists='append', dtype={'date':VARCHAR(df.index.get_level_values('date').str.len().max())})
            df.to_sql(code, connection, if_exists='append', dtype={'date':VARCHAR(df.index.get_level_values('date').str.len().max())})

    def drop_tables(self, namelist):
        if isinstance(namelist, list):
            for name in namelist:
                self.drop_table(name)
        else:
            logger.warning('parameter must be a list')

    def drop_table(self, name):
        sql = sqlalchemy.text('drop table `{}`'.format(name))
        with self.engine.begin() as connection:
            connection.execute(sql)

    def write_stock_table(self, df):
        if isinstance(df, pd.DataFrame):
            with self.engine.begin() as connection:
                df.to_sql('stock_table',connection,if_exists='append')
        else:
            logger.warning('data type not DataFrame')

    def read_stock_table(self):
        logger.info("start read_stock_table")
        with self.engine.begin() as connection:
            df = pd.read_sql('stock_table', connection)
        return df

    def read_db_K_data(self, code):
        logger.info("start read_db_K_data for {}".format(code))
        with self.engine.begin() as connection:
            df = pd.read_sql(code, connection)
            return df

    def del_row(self, code, date):
        sql='select count(*) from `{}` where date="{}"'.format(code, date)
        result=self.engine.execute(sql).scalar()
        if result > 1:
            logger.info("delete row of {}for date {}".format(code, date))
            sql_del='delete from `{}` where date="{}" limit 1'.format(code, date)
            self.engine.execute(sql_del)


class StockOrmHD(StockOrm):
    engine = sqlalchemy.create_engine('mysql://root:password@localhost/stock_history_detail?charset=utf8')
    def __init__(self, ):
        pass

class StockOrmHK(StockOrm):
    engine = sqlalchemy.create_engine('mysql://root:password@localhost/stock?charset=utf8')
    def __init__(self, ):
        pass

class StockOrmQuantFire(StockOrm):
    engine = sqlalchemy.create_engine('mysql://root:password@localhost/stock?charset=utf8')
    def __init__(self, ):
        pass
    def append_fire_date(self, code, dates):
        if self.exist(code, 'quantfire'):
            sql = 'update quantfire set dates=json_array_append(dates,'
            for date in dates:
                sql +='"$","{}",'.format(date)
            sql = sql.strip(',')
            sql +=') where code="{}"'.format(code);
        else:
            sql = 'insert into quantfire (code, dates) values("{}",\'{}\')'.format(code, json.dumps(dates))
        logger.info('sql string is {}'.format(sql))
        with self.engine.begin() as connection:
            connection.execute(sql)


if __name__ == '__main__':
    logging.basicConfig(filename='stock.log', format='%(asctime)s %(message)s', level=logging.INFO)
    a = StockOrmQuantFire()
    code = '000000'
    dates=['0000-00-00','0000-01-01']
    a.append_fire_date(code, dates)
