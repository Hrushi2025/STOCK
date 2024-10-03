import yfinance as yf
import pandas as pd
import mysql.connector
from datetime import datetime, timedelta



def get_stock_data(symbol):
    ticker = yf.Ticker(symbol)
    end_date = datetime.today().strftime('%Y-%m-%d')
    start_date = (datetime.today() - timedelta(days=365)).strftime('%Y-%m-%d')
    data = ticker.history(start=start_date, end=end_date)
    data.reset_index(inplace=True)
    data['SYMBOL'] = symbol
    return data



def insert_data_into_db(data):

    cnx = mysql.connector.connect(user='root', password='test123',
                                  host='localhost', database='stock')
    cursor = cnx.cursor()


    for i, row in data.iterrows():
        cursor.execute("""
            INSERT INTO daily_stock_fact (DATE, SYMBOL, OPEN, HIGH, LOW, CLOSE, `SHARES TRADED`, `TURNOVER (₹ Cr)`)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """, (row['Date'].strftime('%Y-%m-%d'), row['SYMBOL'], row['Open'], row['High'], row['Low'], row['Close'],
              row['Volume'], row['Volume'] * row['Close'] / 10 ** 7))


    cnx.commit()


    cursor.close()
    cnx.close()


def update_table_with_new_symbols(symbols):
    for symbol in symbols:
        data = get_stock_data(symbol)
        insert_data_into_db(data)


new_symbols = ['ANNAPURNA']
update_table_with_new_symbols(new_symbols)
