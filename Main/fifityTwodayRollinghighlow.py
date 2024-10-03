import pandas as pd
from sqlalchemy import create_engine
from datetime import datetime, timedelta
import STOCK.ConfigureFiles.config as cfg
import STOCK.ConfigureFiles.highlowrollConfig as cfg

#MySQL connection
db_details = cfg.db_config

#Create a SQLAlchemy engine
engine = create_engine(f'mysql+mysqlconnector://{db_details["user"]}:{db_details["password"]}@{db_details["host"]}/{db_details["database"]}')

#Step 1: Define the default start date
defaultStat_date = datetime(2023, 1, 1)

#Step 2: Fetch the maximum date from rolling_high_low table
print("Fetching maximum date from rolling_high_low...")
print("Max date query being used:", cfg.max_date_query)
max_date_df = pd.read_sql(cfg.max_date_query, engine)  #Here we are using cfg for configuration
print("Maximum date fetched:", max_date_df)
maxDate = max_date_df['max_date'].values[0]

#Convert max_date to a datetime object if it is not there already
if pd.notna(maxDate):
    max_date = pd.to_datetime(maxDate)  #Here we are ensure it's a Timestamp object

print("Processed max_date:", maxDate)

#Step 3: Determine the start date for fetching data
if pd.isna(maxDate):
#Here we are adjusting max_date and set it as the start date
    adjusted_start_date = maxDate + timedelta(days=1) - timedelta(days=52)
    startDate = adjusted_start_date.strftime('%Y-%m-%d')
else:
#No data in rolling_high_low table,fetch all data from the default start date
    startDate = defaultStat_date.strftime('%Y-%m-%d')

print("Start date for data fetching:",  startDate)
end_date = datetime.now().strftime('%Y-%m-%d')

#Step 4: Fetch data using fetch_data_query from config
print("Fetching data from stock_daily_staging...")
query = cfg.fetch_data_query(startDate, end_date) #Here we are using cfg for configuration
print("Query being executed:", query)
df = pd.read_sql(query, engine)
print("Data fetched from stock_daily_staging:", df.head())  #Displaying the first few rows

#Step 5: Drop unwanted columns
df = df.drop(columns=['VOLUME', 'DIVIDENDS', 'STOCK_SPLITS', 'TURNOVER_Cr'])

#Step 6: Here we are converting the DATE column to datetime type and set as index
print("Processing DATE column...")
df['DATE'] = pd.to_datetime(df['DATE'])
df.set_index('DATE', inplace=True)
print("Data after setting DATE as index:", df.head())  #Displaying the first few rows

#Step 7: So now we are calculating rolling high and low on the CLOSE column
print("Calculating rolling high and low...")
window_size = 52
df['Rolling_High'] = df['CLOSE'].rolling(window=window_size).max()
df['Rolling_Low'] = df['CLOSE'].rolling(window=window_size).min()
print("Data with rolling high and low:", df.head()) #Displaying the first few rows

#Step 8: Reset our index and display the DataFrame
print("Final DataFrame...")
df.reset_index(inplace=True)
print(df.head()) #Displaying the first few rows

#Step 9: Save the results to a CSV file
csv_file_path = '../allCsv'
print(f"Saving results to CSV file: {csv_file_path}...")
df.to_csv(csv_file_path, index=False)
print(f"Data saved to CSV file: {csv_file_path}.")
