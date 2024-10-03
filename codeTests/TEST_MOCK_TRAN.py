import pymysql
import random
from datetime import datetime, timedelta


# Database connection
def connect_db():
    return pymysql.connect(
        host='localhost',
        user='root',
        password='test123',
        database='stock',
        cursorclass=pymysql.cursors.DictCursor
    )


# Fetch member-symbol assignments from the database
def fetch_member_symbol_data(conn):
    query = "SELECT member_id, symbol FROM member_symbol_assignment;"
    with conn.cursor() as cursor:
        cursor.execute(query)
        results = cursor.fetchall()
        return results


# Fetch valid trading days from the trading_days table for a specific date
def fetch_trading_day(conn, date):
    query = "SELECT date FROM trading_days WHERE date = %s;"
    with conn.cursor() as cursor:
        cursor.execute(query, (date,))
        result = cursor.fetchone()
        return result['date'] if result else None


# Fetch the close price for a specific symbol on a specific trading day
def fetch_close_price(conn, symbol, date):
    query = "SELECT close FROM stock_daily_fact WHERE symbol = %s AND date = %s;"
    with conn.cursor() as cursor:
        cursor.execute(query, (symbol, date))
        result = cursor.fetchone()
        return result['close'] if result else None


# Insert signals into the database
def insert_signals(conn, signals):
    insert_query = "INSERT INTO member_close_buy_sell (member_id, symbol, action, qty, date, close) VALUES (%s, %s, %s, %s, %s, %s);"
    with conn.cursor() as cursor:
        cursor.executemany(insert_query, signals)
        conn.commit()


# Generate buy/sell signals for a specific date
# Generate buy/sell signals for a specific date
def generate_daily_signals(conn, current_date, member_symbols):
    signals = []
    member_ids = list(member_symbols.keys())

    # Ensure we generate exactly 10 transactions
    while len(signals) < 10:
        member_id = random.choice(member_ids)  # Randomly select a member
        symbols = member_symbols[member_id]  # Get their symbols

        if not symbols:
            continue  # Skip if no symbols

        symbol = random.choice(symbols)  # Randomly select a symbol
        action = random.choice(['buy', 'sell'])  # Randomly choose action

        close_price = fetch_close_price(conn, symbol, current_date)
        if close_price:
            if action == 'buy':
                qty = random.randint(4, 5)  # Buy qty between 4 and 5
                signals.append((member_id, symbol, 'buy', qty, current_date.strftime('%Y-%m-%d'), close_price))
                print(f"Buying {symbol} for member {member_id} on {current_date}")

            elif action == 'sell':
                qty = random.randint(1, 3)  # Sell qty between 1 and 3
                signals.append((member_id, symbol, 'sell', qty, current_date.strftime('%Y-%m-%d'), close_price))
                print(f"Selling {symbol} for member {member_id} on {current_date}")

    print(f"Generated {len(signals)} signals for {current_date}")
    return signals



# Fetch the maximum date from stock_daily_fact
def fetch_max_date(conn):
    query = "SELECT MAX(date) AS max_date FROM stock_daily_fact;"
    with conn.cursor() as cursor:
        cursor.execute(query)
        result = cursor.fetchone()
        return result['max_date'] if result else None


# Fetch last transaction date from member_close_buy_sell
def fetch_last_transaction_date(conn):
    query = "SELECT MAX(date) AS last_date FROM member_close_buy_sell;"
    with conn.cursor() as cursor:
        cursor.execute(query)
        result = cursor.fetchone()
        return result['last_date'] if result else None


# Process data daily until the maximum available date
def process_data_until_today(conn, member_symbols):
    last_transaction_date = fetch_last_transaction_date(conn)
    max_date = fetch_max_date(conn)

    current_date = last_transaction_date + timedelta(days=1)

    while current_date <= max_date:
        if fetch_trading_day(conn, current_date):
            print(f"Processing signals for {current_date}...")
            signals = generate_daily_signals(conn, current_date, member_symbols)
            insert_signals(conn, signals)

        current_date += timedelta(days=1)


# Main function
def main():
    conn = connect_db()

    try:
        # Fetch member-symbol data
        member_symbol_data = fetch_member_symbol_data(conn)

        # Build a dictionary where member_id is the key and the value is a list of symbols assigned to that member
        member_symbols = {}
        for record in member_symbol_data:
            member_id = record['member_id']
            symbol = record['symbol']
            if member_id not in member_symbols:
                member_symbols[member_id] = []
            member_symbols[member_id].append(symbol)

        # Process the data until today
        process_data_until_today(conn, member_symbols)

    finally:
        conn.close()


if __name__ == "__main__":
    main()