# config.py

# Example database configuration
db_config = {
    'user': 'root',
    'password': 'test123',
    'host': 'localhost',
    'database': 'stock'
}

# SQL query to get the maximum date
max_date_query = "SELECT MAX(DATE) AS max_date FROM rolling_high_low"

# Function to generate a query for fetching data between start_date and end_date
def fetch_data_query(start_date, end_date):
    return f"""
    SELECT *
    FROM stock_daily_staging
    WHERE DATE BETWEEN '{start_date}' AND '{end_date}'
    """
