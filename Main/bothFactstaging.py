import yfinance as yf
import pandas as pd
import datetime as dt
import sqlalchemy as sq
import time

# Creating SQLAlchemy engine to connect to the MySQL database
engine = sq.create_engine("mysql+pymysql://root:test123@localhost:3306/stock", pool_size=10, max_overflow=20)

# Function to fetch stock data from Yahoo Finance
def fetch_stock_data(start_date: str, end_date: str, symbols: list) -> list:
    records = []
    for sym in symbols:
        try:
            ticker = yf.Ticker(sym + '.NS')
            stock_data = ticker.history(start=start_date, end=end_date)

            if stock_data.empty:
                print(f"No data found for {sym} from {start_date} to {end_date}")
                continue

            for date, row in stock_data.iterrows():
                records.append([
                    date.strftime("%Y-%m-%d"),
                    sym,
                    float(row['Open']),
                    float(row['High']),
                    float(row['Low']),
                    float(row['Close']),
                    int(row['Volume']),
                    float((row['Close'] * row['Volume']) / 10 ** 7),  # Turnover in crores
                    float(row['Dividends']),
                    float(row['Stock Splits'])
                ])
        except Exception as e:
            print(f"Error fetching data for {sym}: {e}")
        time.sleep(1)  # Sleep to avoid hitting rate limits
    return records

# Function to create DataFrames from the fetched stock data and the symbol dimension file
def create_dataframes(start_date: str, end_date: str):
    records = fetch_stock_data(start_date, end_date, symbols_list)
    if not records:
        print("No records to process.")
        return pd.DataFrame(), pd.DataFrame()

    stock_data_df = pd.DataFrame(records, columns=['DATE', 'SYMBOL', 'OPEN', 'HIGH', 'LOW', 'CLOSE', 'VOLUME', 'TURNOVER_Cr', 'DIVIDENDS', 'STOCK_SPLITS'])

    symbol_data = pd.read_csv(r'/STOCK/Dirfiles/symbol_dimension.csv')

    merged_df = pd.merge(symbol_data, stock_data_df, on='SYMBOL', how='inner')

    trading_days_query = pd.read_sql_query("SELECT date FROM trading_days", engine)
    trading_days = pd.to_datetime(trading_days_query['date']).dt.date

    merged_df['DATE'] = pd.to_datetime(merged_df['DATE']).dt.date

    valid_data_df = merged_df[merged_df['DATE'].isin(trading_days)]

    staging_df = valid_data_df[['SYMBOL_ID', 'DATE', 'SYMBOL', 'OPEN', 'HIGH', 'LOW', 'CLOSE', 'VOLUME', 'DIVIDENDS', 'STOCK_SPLITS', 'TURNOVER_Cr']]
    fact_df = valid_data_df[['SYMBOL_ID', 'DATE', 'SYMBOL', 'SERIES', 'OPEN', 'HIGH', 'LOW', 'CLOSE']]

    return fact_df, staging_df

# Function to save the DataFrames to the MySQL database
def save_to_sql(start_date: str, end_date: str):
    fact_df, staging_df = create_dataframes(start_date, end_date)

    if not fact_df.empty:
        fact_df.to_sql("stock_daily_fact", engine, if_exists="append", index=False)
    if not staging_df.empty:
        staging_df.to_sql("stock_daily_staging", engine, if_exists='append', index=False)

    print("Data successfully saved to the database.")

# Main function to orchestrate the data fetching and storing process
def main(start_date: str):
    max_dt_query = pd.read_sql_query("SELECT MAX(DATE) as max_date FROM stock_daily_staging", engine)
    max_dt = max_dt_query['max_date'].iloc[0]

    if pd.isnull(max_dt):
        max_dt = dt.datetime.strptime(start_date, '%Y-%m-%d').date()
    else:
        max_dt = pd.to_datetime(max_dt).date()

    today_date = dt.date.today()
    if max_dt < today_date:
        start_date = max_dt + dt.timedelta(days=1)
        end_date = today_date
    else:
        start_date = today_date
        end_date = today_date

    global symbols_list
    nse_symbols = pd.read_csv(r'/STOCK/Dirfiles/symbol_dimension.csv')
    symbols_list = nse_symbols['SYMBOL'].to_list()

    save_to_sql(start_date, end_date)

if __name__ == '__main__':
    start_date = '2023-01-01'  # Example start date
    main(start_date)
