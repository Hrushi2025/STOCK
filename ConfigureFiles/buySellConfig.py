# In STOCK/ConfigureFiles/buySellConfig.py

# SQL Query Constants
GET_MEMBER_SYMBOL_DATA_QUERY = "SELECT member_id, symbol FROM member_symbol_assignment"  # Adjust this based on your database schema
FETCH_TRADING_DAY_QUERY = "SELECT date FROM trading_days WHERE date = %s"
FETCH_CLOSE_PRICE_QUERY = "SELECT close FROM stock_daily_fact WHERE symbol = %s AND date = %s"
GET_MAX_DATE_QUERY = "SELECT MAX(date) AS max_date FROM trading_days"
GET_MIN_DATE_QUERY = "SELECT MIN(date) AS min_date FROM trading_days"
INSERT_SIGNALS_QUERY = "INSERT INTO member_close_buy_sell (member_id, symbol, action, qty, date, close) VALUES (%s, %s, %s, %s, %s, %s)"


import pymysql

# Function to establish a database connection
def connect_db():
    return pymysql.connect(
        host='localhost',
        user='root',
        password='test123',
        database='stock',
        cursorclass=pymysql.cursors.DictCursor
    )

