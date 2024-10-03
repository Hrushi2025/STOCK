import pandas as pd
import mysql.connector

df = pd.read_csv(r'/STOCK/INPUT/buy_sell_alaram.csv')

df['DATE'] = pd.to_datetime(df['DATE'])
start_date = '2024-07-03'
end_date = '2024-09-03'
df = df[(df['DATE'] >= start_date) & (df['DATE'] <= end_date)]

df['Buy_Signal'] = ''
df['Sell_Signal'] = ''


for symbol in df['SYMBOL'].unique():
    symbol_df = df[df['SYMBOL'] == symbol]

    bought = False

    for i in range(1, len(symbol_df)):
        current_row = symbol_df.iloc[i]
        prev_row = symbol_df.iloc[i - 1]


        if not bought and current_row['RSI_14'] < 30:
            df.loc[current_row.name, 'Buy_Signal'] = 'BUY'
            bought = True


        elif bought and current_row['RSI_14'] > 70:
            df.loc[current_row.name, 'Sell_Signal'] = 'SELL'
            bought = False


connection = mysql.connector.connect(
    host='localhost',
    user='root',
    password='test123',
    database='stock'
)

cursor = connection.cursor()


for index, row in df.iterrows():
    sql_update_query = """
    UPDATE buy_sell_alarm
    SET Buy_Signal = %s, Sell_Signal = %s
    WHERE DATE = %s AND SYMBOL = %s
    """
    data = (row['Buy_Signal'], row['Sell_Signal'], row['DATE'].strftime('%Y-%m-%d'), row['SYMBOL'])
    cursor.execute(sql_update_query, data)


connection.commit()


cursor.close()
connection.close()
