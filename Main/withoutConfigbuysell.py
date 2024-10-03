import pymysql  # MySQL database connector
import random  # For generating random choices
from datetime import datetime, timedelta  # For date manipulations

# Function to establish a database connection
def connectDB():
    return pymysql.connect(
        host='localhost',
        user='root',
        password='test123',
        database='stock',
        cursorclass=pymysql.cursors.DictCursor  # Using DictCursor for dictionary-like access
    )

# Function to fetch member-symbol assignments from the database
def fetchMembersymbolData(conn):
    query = "SELECT member_id, symbol FROM member_symbol_assignment;"
    with conn.cursor() as cursor:
        cursor.execute(query)  # Execute the query
        results = cursor.fetchall()  # Fetch all results
        return results  # Return the results

# Function to check if a specific date is a valid trading day
def fetchTradingday(conn, date):
    query = "SELECT date FROM trading_days WHERE date = %s;"
    with conn.cursor() as cursor:
        cursor.execute(query, (date,))  # Execute the query with the provided date
        result = cursor.fetchone()  # Fetch the result
        return result['date'] if result else None  # Return the date if found

# Function to fetch the close price of a specific symbol on a specific date
def fetchCloseprice(conn, symbol, date):
    query = "SELECT close FROM stock_daily_fact WHERE symbol = %s AND date = %s;"
    with conn.cursor() as cursor:
        cursor.execute(query, (symbol, date))  # Execute the query with the symbol and date
        result = cursor.fetchone()  # Fetch the result
        return result['close'] if result else None  # Return the close price if found

# Function to insert generated buy/sell signals into the database
def insertSignals(conn, signals):
    insert_query = "INSERT INTO member_close_buy_sell (member_id, symbol, action, qty, date, close) VALUES (%s, %s, %s, %s, %s, %s);"
    with conn.cursor() as cursor:
        cursor.executemany(insert_query, signals)  # Insert multiple signals at once
        conn.commit()  # Commit the transaction

# Function to generate buy/sell signals for a specific date
def generateDailysignals(conn, current_date, member_symbols):
    signals = []  # List to store generated signals
    member_ids = list(member_symbols.keys())  # Get all member IDs

    # Loop to ensure we generate exactly 10 transactions
    while len(signals) < 10:
        member_id = random.choice(member_ids)  # Randomly select a member
        symbols = member_symbols[member_id]  # Get symbols associated with that member

        if not symbols:
            continue  # Skip if the member has no symbols

        symbol = random.choice(symbols)  # Randomly select a symbol
        action = random.choice(['buy', 'sell'])  # Randomly choose to buy or sell

        close_price = fetchCloseprice(conn, symbol, current_date)  # Get the close price for the selected symbol
        if close_price:  # Proceed if a valid close price is retrieved
            if action == 'buy':
                qty = random.randint(4, 5)  # Buy quantity between 4 and 5
                signals.append((member_id, symbol, 'buy', qty, current_date.strftime('%Y-%m-%d'), close_price))  # Add buy signal
                print(f"Buying {symbol} for member {member_id} on {current_date}")

            elif action == 'sell':
                qty = random.randint(1, 3)  # Sell quantity between 1 and 3
                signals.append((member_id, symbol, 'sell', qty, current_date.strftime('%Y-%m-%d'), close_price))  # Add sell signal
                print(f"Selling {symbol} for member {member_id} on {current_date}")

    print(f"Generated {len(signals)} signals for {current_date}")  # Log the number of signals generated
    return signals  # Return the list of generated signals

# Function to fetch the maximum date available in the stock_daily_fact table
def fetchMaxdate(conn):
    query = "SELECT MAX(date) AS max_date FROM stock_daily_fact;"
    with conn.cursor() as cursor:
        cursor.execute(query)  # Execute the query
        result = cursor.fetchone()  # Fetch the result
        return result['max_date'] if result else None  # Return the maximum date if found

# Function to fetch the last transaction date from member_close_buy_sell
def fetchLasttransactionDdate(conn):
    query = "SELECT MAX(date) AS last_date FROM member_close_buy_sell;"
    with conn.cursor() as cursor:
        cursor.execute(query)  # Execute the query
        result = cursor.fetchone()  # Fetch the result
        return result['last_date'] if result else None  # Return the last transaction date if found

# Function to process trading data daily until the maximum date available
def processDatauntilToday(conn, member_symbols):
    last_transaction_date = fetchLasttransactionDdate(conn)  # Get the last transaction date
    max_date = fetchMaxdate(conn)  # Get the maximum available date

    current_date = last_transaction_date + timedelta(days=1)  # Start processing from the day after the last transaction

    # Loop through each date until the maximum date
    while current_date <= max_date:
        if fetchTradingday(conn, current_date):  # Check if the current date is a trading day
            print(f"Processing signals for {current_date}...")  # Log the current date being processed
            signals = generateDailysignals(conn, current_date, member_symbols)  # Generate signals for the date
            insertSignals(conn, signals)  # Insert the generated signals into the database

        current_date += timedelta(days=1)  # Move to the next day

# Main function to execute the script
def main():
    conn = connectDB()  # Establish a database connection

    try:
        # Fetch member-symbol data from the database
        member_symbol_data = fetchMembersymbolData(conn)

        # Build a dictionary where member_id is the key and the value is a list of symbols assigned to that member
        member_symbols = {}
        for record in member_symbol_data:
            member_id = record['member_id']  # Get member ID
            symbol = record['symbol']  # Get associated symbol
            if member_id not in member_symbols:
                member_symbols[member_id] = []  # Create a new entry for the member if not existing
            member_symbols[member_id].append(symbol)  # Append the symbol to the member's list

        # Process the trading data until today
        processDatauntilToday(conn, member_symbols)

    finally:
        conn.close()  # Ensure the database connection is closed

# Entry point of the script
if __name__ == "__main__":
    main()  # Run the main function
