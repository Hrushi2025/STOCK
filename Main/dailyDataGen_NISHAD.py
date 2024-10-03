import pandas as pd
import datetime as dt
from STOCK.ConfigureFiles.config import db_config
import yfinance as yf

# Set up cursor for database interaction
cursor = db_config.cursor()
pd.set_option("display.max_columns", None)


def getSymbols():
    cursor.execute("SELECT symbol FROM symbol_dimension")
    data = cursor.fetchall()
    symbols = [item[0] for item in data]
    print("Symbols fetched:", symbols)
    return symbols


def getMaxDate():
    cursor.execute("SELECT MAX(date) FROM stock_daily_staging")
    date = cursor.fetchall()[0][0]
    print("Max date fetched:", date)
    return date


def getStockData(symbols, start_date, end_date):
    print("Getting stock data...")
    record = []
    for sym in symbols:
        sdata = yf.Ticker(sym + ".NS").history(start=start_date, end=end_date)
        for date, data in sdata.iterrows():
            record.append([
                date.strftime("%Y-%m-%d"),
                sym,
                data['Open'],
                data['High'],
                data['Low'],
                data['Close'],
                data['Volume'],
                data['Dividends'],
                data['Stock Splits'],
                (data['Close'] * data['Volume']) / 11 ** 7
            ])
    return record


def dataFrames(record):
    staging_data = pd.DataFrame(record,
                                columns=['DATE', 'SYMBOL', 'OPEN', 'HIGH', 'LOW', 'CLOSE', 'VOLUME', 'DIVIDENDS',
                                         'STOCK_SPLITS', 'TURNOVER_Cr'])

    cursor.execute("SELECT symbol_id, SYMBOL FROM symbol_dimension")  # Exclude 'series' temporarily for checking
    symbol_data = cursor.fetchall()

    # Create DataFrame for symbol data
    symbol_df = pd.DataFrame(symbol_data, columns=['symbol_id', 'SYMBOL'])

    # Merge with staging data
    merged_data = pd.merge(staging_data, symbol_df, left_on="SYMBOL", right_on="SYMBOL", how='inner')

    # Create staging and fact tables
    staging = merged_data[
        ['symbol_id', 'DATE', 'SYMBOL', 'OPEN', 'HIGH', 'LOW', 'CLOSE', 'VOLUME', 'DIVIDENDS', 'STOCK_SPLITS',
         'TURNOVER_Cr']]
    fact = merged_data[['symbol_id', 'DATE', 'SYMBOL', 'OPEN', 'HIGH', 'LOW', 'CLOSE']]  # Exclude series

    return (staging, fact)


def toSql(staging, fact):
    staging.sort_values(['DATE', 'symbol_id'], inplace=True)
    fact.sort_values(['DATE', 'symbol_id'], inplace=True)

    print("Staging data:", staging, "\nFact data:", fact)

    insert_staging = """INSERT INTO stock_daily_staging 
                        (symbol_id, DATE, SYMBOL, OPEN, HIGH, LOW, CLOSE, VOLUME, DIVIDENDS, STOCK_SPLITS, TURNOVER_Cr)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""

    for item, row in staging.iterrows():
        cursor.execute(insert_staging, tuple(row))
    db_config.commit()

    insert_fact = """INSERT INTO stock_daily_fact 
                     (symbol_id, DATE, SYMBOL, OPEN, HIGH, LOW, CLOSE)
                     VALUES (%s, %s, %s, %s, %s, %s, %s)"""

    for item, row in fact.iterrows():
        cursor.execute(insert_fact, tuple(row))
    db_config.commit()


def main():
    symbols = getSymbols()

    start_date = getMaxDate() + dt.timedelta(days=1)
    if start_date >= dt.date.today():
        print("Start date is today's date or beyond; setting to today.")
        start_date = dt.date.today()
        end_date = start_date + dt.timedelta(days=1)
    else:
        end_date = dt.date.today() + dt.timedelta(days=1)

    print("Start date:", start_date, "End date:", end_date)
    record = getStockData(symbols, start_date, end_date)
    staging, fact = dataFrames(record)
    toSql(staging, fact)


if __name__ == "__main__":
    main()
