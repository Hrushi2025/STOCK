import random
from datetime import datetime, timedelta
from STOCK.ConfigureFiles.buySellConfig import (
    connect_db,
    GET_MEMBER_SYMBOL_DATA_QUERY,
    FETCH_TRADING_DAY_QUERY,
    FETCH_CLOSE_PRICE_QUERY,
    GET_MAX_DATE_QUERY,
    GET_MIN_DATE_QUERY,
    INSERT_SIGNALS_QUERY,
)

# Add the fetchAllClosePrices function here
def fetchAllClosePrices(conn):
    query = "SELECT symbol, date, close FROM stock_daily_fact"  # Adjust this query as needed
    with conn.cursor() as cursor:
        cursor.execute(query)
        results = cursor.fetchall()

    # Organizing close prices in a dictionary for quick access
    close_prices = {}
    for row in results:
        key = (row['symbol'], row['date'])
        close_prices[key] = row['close']

    return close_prices

def fetchMembersymbolData(conn):
    with conn.cursor() as cursor:
        cursor.execute(GET_MEMBER_SYMBOL_DATA_QUERY)
        results = cursor.fetchall()
    return [(row['member_id'], row['symbol']) for row in results]

def fetchTradingDay(conn, date):
    with conn.cursor() as cursor:
        cursor.execute(FETCH_TRADING_DAY_QUERY, (date,))
        result = cursor.fetchone()
    return result

def fetchClosePrice(conn, symbol, date):
    with conn.cursor() as cursor:
        cursor.execute(FETCH_CLOSE_PRICE_QUERY, (symbol, date))
        result = cursor.fetchone()
    return result['close'] if result else None

def insertSignals(conn, signals):
    with conn.cursor() as cursor:
        cursor.executemany(INSERT_SIGNALS_QUERY, signals)
    conn.commit()

def generateDailyTransactions(current_date, member_symbols, close_prices, transaction_count=1000):
    transactions = []
    total_pairs = len(member_symbols)

    if total_pairs == 0:
        print("No member-symbol pairs available.")
        return transactions

    for _ in range(transaction_count):
        member_id, symbol = random.choice(member_symbols)

        action = random.choice(['buy', 'sell'])

        close_price = close_prices.get((symbol, current_date))
        if close_price is None:
            continue

        qty = random.randint(4, 5) if action == 'buy' else random.randint(1, 3)

        transactions.append(
            (member_id, symbol, action, qty, current_date.strftime('%Y-%m-%d'), close_price)
        )

    return transactions

def fetchMaxDate(conn):
    with conn.cursor() as cursor:
        cursor.execute(GET_MAX_DATE_QUERY)
        result = cursor.fetchone()
    return result['max_date'] if result else None

def fetchMinDate(conn):
    with conn.cursor() as cursor:
        cursor.execute(GET_MIN_DATE_QUERY)
        result = cursor.fetchone()
    return result['min_date'] if result else None

def processDataUntilToday(conn, member_symbols, trading_days, close_prices, transaction_count=1000, batch_size=10000):
    transactions_batch = []

    for current_date in trading_days:
        daily_transactions = generateDailyTransactions(
            current_date,
            member_symbols,
            close_prices,
            transaction_count=transaction_count
        )

        transactions_batch.extend(daily_transactions)

        if len(transactions_batch) >= batch_size:
            insertSignals(conn, transactions_batch)
            print(f"Inserted {len(transactions_batch)} transactions up to {current_date}")
            transactions_batch = []

    if transactions_batch:
        insertSignals(conn, transactions_batch)
        print(f"Inserted remaining {len(transactions_batch)} transactions up to {trading_days[-1]}")

def main():
    conn = connect_db()

    try:
        member_symbol_data = fetchMembersymbolData(conn)
        member_symbols = member_symbol_data  # Already a list of (member_id, symbol)

        trading_days = []
        current_date = fetchMinDate(conn)
        max_date = fetchMaxDate(conn)

        while current_date <= max_date:
            if fetchTradingDay(conn, current_date):
                trading_days.append(current_date)
            current_date += timedelta(days=1)

        trading_days.sort()  # Ensure dates are in order

        close_prices = fetchAllClosePrices(conn)

        processDataUntilToday(
            conn,
            member_symbols,
            trading_days,
            close_prices,
            transaction_count=1000,
            batch_size=50000
        )

    finally:
        conn.close()

if __name__ == "__main__":
    main()
