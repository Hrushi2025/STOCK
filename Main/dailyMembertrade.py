import random  # Import the random module for generating random values
from datetime import datetime, timedelta  # Import datetime and timedelta for date manipulation
from STOCK.ConfigureFiles.buySellConfig import (
    connect_db,  # Function to connect to the database
    GET_MEMBER_SYMBOL_DATA_QUERY,  # SQL query to fetch member and symbol data
    FETCH_TRADING_DAY_QUERY,  # SQL query to fetch trading day information
    FETCH_CLOSE_PRICE_QUERY,  # SQL query to fetch close price for a specific symbol on a given date
    GET_MAX_DATE_QUERY,  # SQL query to get the maximum date from the dataset
    GET_MIN_DATE_QUERY,  # SQL query to get the minimum date from the dataset
    INSERT_SIGNALS_QUERY,  # SQL query to insert signals into the database
)

# Function to fetch all close prices from the database
def fetchAllClosePrices(conn):
    query = "SELECT symbol, date, close FROM stock_daily_fact"  # Source SQL query to get close prices
    with conn.cursor() as cursor:  # Create a cursor for executing the query
        cursor.execute(query)  # Execute the SQL query
        results = cursor.fetchall()  # Fetch all results from the query

    # Organizing close prices in a dictionary for quick access (target structure)
    close_prices = {}
    for row in results:  # Loop through each row of the results
        key = (row['symbol'], row['date'])  # Create a key from symbol and date
        close_prices[key] = row['close']  # Assign close price to the key in the dictionary

    return close_prices  # Return the dictionary of close prices

# Function to fetch member-symbol data from the database
def fetchMemberSymbolData(conn):
    with conn.cursor() as cursor:  # Create a cursor for executing the query
        cursor.execute(GET_MEMBER_SYMBOL_DATA_QUERY)  # Execute the member-symbol query
        results = cursor.fetchall()  # Fetch all results from the query
    return [(row['member_id'], row['symbol']) for row in results]  # Target: return list of (member_id, symbol) tuples

# Function to fetch trading day information for a given date
def fetchTradingDay(conn, date):
    with conn.cursor() as cursor:  # Create a cursor for executing the query
        cursor.execute(FETCH_TRADING_DAY_QUERY, (date,))  # Execute the query for the specific date
        result = cursor.fetchone()  # Fetch the single result from the query
    return result  # Return the trading day information

# Function to fetch the close price for a specific symbol on a given date
def fetchClosePrice(conn, symbol, date):
    with conn.cursor() as cursor:  # Create a cursor for executing the query
        cursor.execute(FETCH_CLOSE_PRICE_QUERY, (symbol, date))  # Execute the query with symbol and date
        result = cursor.fetchone()  # Fetch the single result from the query
    return result['close'] if result else None  # Return close price if available, else None

# Function to insert generated signals into the database
def insertSignals(conn, signals):
    with conn.cursor() as cursor:  # Create a cursor for executing the insertion
        cursor.executemany(INSERT_SIGNALS_QUERY, signals)  # Insert multiple signals into the database
    conn.commit()  # Commit the transaction to the database

# Function to generate daily transactions for members
def generateDailyTransactions(current_date, member_symbols, close_prices, transaction_count=1000):
    transactions = []  # Initialize an empty list for transactions
    total_pairs = len(member_symbols)  # Get the total number of member-symbol pairs

    if total_pairs == 0:  # Check if there are no member-symbol pairs
        print("No member-symbol pairs available.")  # Print message if no pairs found
        return transactions  # Return empty transactions

    for _ in range(transaction_count):  # Loop to generate a specified number of transactions
        member_id, symbol = random.choice(member_symbols)  # Source: randomly select a member-symbol pair

        action = random.choice(['buy', 'sell'])  # Source: randomly select an action (buy/sell)

        close_price = close_prices.get((symbol, current_date))  # Target: fetch close price for the current date
        if close_price is None:  # Check if close price is not available
            continue  # Skip to the next iteration if no close price

        qty = random.randint(4, 5) if action == 'buy' else random.randint(1, 3)  # Source: generate random quantity based on action

        transactions.append(  # Target: append a new transaction to the list
            (member_id, symbol, action, qty, current_date.strftime('%Y-%m-%d'), close_price)  # Transaction details
        )

    return transactions  # Return the list of generated transactions

# Function to fetch the maximum date from the dataset
def fetchMaxDate(conn):
    with conn.cursor() as cursor:  # Create a cursor for executing the query
        cursor.execute(GET_MAX_DATE_QUERY)  # Execute the query to fetch the maximum date
        result = cursor.fetchone()  # Fetch the single result from the query
    return result['max_date'] if result else None  # Return max date if available, else None

# Function to fetch the minimum date from the dataset
def fetchMinDate(conn):
    with conn.cursor() as cursor:  # Create a cursor for executing the query
        cursor.execute(GET_MIN_DATE_QUERY)  # Execute the query to fetch the minimum date
        result = cursor.fetchone()  # Fetch the single result from the query
    return result['min_date'] if result else None  # Return min date if available, else None

# Function to process data until today and insert transactions into the database
def processDataUntilToday(conn, member_symbols, trading_days, close_prices, transaction_count=1000, batch_size=10000):
    transactions_batch = []  # Initialize an empty list for transaction batches

    for current_date in trading_days:  # Loop through each trading day
        daily_transactions = generateDailyTransactions(  # Target: generate daily transactions for the current date
            current_date,
            member_symbols,
            close_prices,
            transaction_count=transaction_count
        )

        transactions_batch.extend(daily_transactions)  # Add generated transactions to the batch

        if len(transactions_batch) >= batch_size:  # Check if the batch size limit is reached
            insertSignals(conn, transactions_batch)  # Target: insert transactions into the database
            print(f"Inserted {len(transactions_batch)} transactions up to {current_date}")  # Print message with count
            transactions_batch = []  # Reset the batch after insertion

    if transactions_batch:  # Check if there are remaining transactions to insert
        insertSignals(conn, transactions_batch)  # Target: insert remaining transactions
        print(f"Inserted remaining {len(transactions_batch)} transactions up to {trading_days[-1]}")  # Print message with count

# Main function to execute the transaction generation and insertion process
def main():
    conn = connect_db()  # Source: establish connection to the database

    try:
        member_symbol_data = fetchMemberSymbolData(conn)  # Fetch member-symbol data
        member_symbols = member_symbol_data  # Target: already a list of (member_id, symbol)

        trading_days = []  # Initialize an empty list for trading days
        current_date = fetchMinDate(conn)  # Source: fetch minimum date from the database
        max_date = fetchMaxDate(conn)  # Source: fetch maximum date from the database

        while current_date <= max_date:  # Loop through dates from min to max
            if fetchTradingDay(conn, current_date):  # Check if the current date is a trading day
                trading_days.append(current_date)  # Target: append the trading day to the list
            current_date += timedelta(days=1)  # Increment the date by one day

        trading_days.sort()  # Ensure dates are in order (target structure)

        close_prices = fetchAllClosePrices(conn)  # Fetch all close prices

        processDataUntilToday(  # Process data and insert transactions
            conn,
            member_symbols,
            trading_days,
            close_prices,
            transaction_count=1000,
            batch_size=50000
        )

    finally:
        conn.close()  # Ensure the database connection is closed

# Entry point of the script
if __name__ == "__main__":
    main()  # Execute the main function
