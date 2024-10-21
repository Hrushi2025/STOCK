# In STOCK/ConfigureFiles/buySellConfig.py

# SQL Query Constants
GET_MEMBER_SYMBOL_DATA_QUERY = "SELECT member_id, symbol FROM member_symbol_assignment"  # query to get member_id and symbol from the member_symbol_assignment table
FETCH_TRADING_DAY_QUERY = "SELECT date FROM trading_days WHERE date = %s"  # query to select date from trading_days where date matches the provided parameter
FETCH_CLOSE_PRICE_QUERY = "SELECT close FROM stock_daily_fact WHERE symbol = %s AND date = %s"  # query to select close price for a specific symbol and date
GET_MAX_DATE_QUERY = "SELECT MAX(date) AS max_date FROM trading_days"  # query to get the maximum date from trading_days table
GET_MIN_DATE_QUERY = "SELECT MIN(date) AS min_date FROM trading_days"  # query to get the minimum date from trading_days table
INSERT_SIGNALS_QUERY = "INSERT INTO member_close_buy_sell (member_id, symbol, action, qty, date, close) VALUES (%s, %s, %s, %s, %s, %s)"  # query to insert signals into member_close_buy_sell table

import pymysql


def connect_db():
    return pymysql.connect(
        host='localhost',
        user='root',
        password='test123',
        database='stock',
        cursorclass=pymysql.cursors.DictCursor
    )
