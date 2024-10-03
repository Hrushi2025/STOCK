import pandas as pd
import random
import numpy as np
from datetime import datetime, timedelta
from colorama import init, Fore
from sqlalchemy import create_engine

init(autoreset=True)

DATABASE_URI = 'mysql+pymysql://root:test123@localhost:3306/stock'
engine = create_engine(DATABASE_URI)


def find_latest_transaction_and_balance(member_id, stock_id):
    buy_df = pd.read_sql_query("SELECT * FROM transactions", engine)

    # Filter buy transactions for the specific member and stock
    member_stock_transactions = buy_df[(buy_df['MEMBER_ID'] == member_id) & (buy_df['SYMBOL'] == stock_id)]

    if member_stock_transactions.empty:
        latest_transaction = None
    else:
        # Find the latest transaction for the member and stock
        latest_transaction = member_stock_transactions.sort_values(by='date', ascending=False).iloc[0]

    return latest_transaction


def update_bal(updated_bal, member_id):
    df = pd.read_sql_query("SELECT * FROM member_dimension", engine)

    if member_id in df['MEMBER_ID'].values:
        df.loc[df['MEMBER_ID'] == member_id, 'balance'] = updated_bal

        df.to_sql('member_dimension', con=engine, if_exists='replace', index=False)


def get_price(symbol, date):
    df = pd.read_sql_query("SELECT * FROM stock_daily_fact", engine)

    df['DATE'] = pd.to_datetime(df['DATE']).dt.strftime('%Y-%m-%d')
    date = pd.to_datetime(date).strftime('%Y-%m-%d')

    filtered_df = df[(df['SYMBOL'] == symbol) & (df['DATE'] == date)]

    if filtered_df.empty:
        return None

    high_price = filtered_df['HIGH'].values[0]
    low_price = filtered_df['LOW'].values[0]

    # Choose a random price between high and low
    random_price = round(random.uniform(low_price, high_price))

    return random_price


def get_balance(member_id):
    df = pd.read_sql_query("SELECT * FROM member_dimension", engine)
    filtered_df = df[(df['MEMBER_ID'] == member_id)]

    if filtered_df.empty:
        return None

    balance = filtered_df['balance'].values[0]

    return balance


def buyStock(bal, val):
    if bal <= 0:
        return 0
    else:
        updated_balance = bal - val
        if updated_balance < 0:
            return 0
        else:
            return updated_balance


# Define the sellStock function
def sellStock(balance, value):
    return balance + value  # Selling increases the balance by the value of the sold stock


# Dictionary to track how much stock each member holds
holdings = {}


def make_transactions():
    # Fetch member data
    member_df = pd.read_sql_query("SELECT * FROM member_dimension limit 5", engine)
    print("Members DataFrame:")
    print(member_df)

    members = member_df['MEMBER_ID']

    # Fetch symbol data
    symbol_df = pd.read_sql_query("SELECT * FROM symbol_dimension limit 7", engine)
    print("Symbols DataFrame:")
    print(symbol_df)

    symbols = symbol_df['SYMBOL']

    # Fetch trading days data
    trading_days_df = pd.read_sql_query("SELECT * FROM trading_days", engine)
    print("Trading Days DataFrame:")
    print(trading_days_df)

    trading_days = trading_days_df['date']

    transactions = pd.DataFrame(columns=['MEMBER_ID', 'SYMBOL', 'action', 'qty', 'date',
                                         'remaining_qty', 'CLOSE', 'current_price',
                                         'total_buy_qty', 'total_sell_qty', 'total_investment',
                                         'total_sell', 'average_investment', 'current_investment', 'profit'])

    start_date = datetime(2024, 8, 28)
    end_date = datetime.today() - timedelta(days=1)
    diff = (end_date - start_date).days

    for day in range(diff + 1):
        current_date = start_date + timedelta(days=day)

        if current_date not in trading_days.values:
            sample_size = min(10, len(members))
            member_ids = np.random.choice(members, size=sample_size, replace=False)

            for member_id in member_ids:
                trans = 1

                for tran in range(trans):
                    symbol = random.choice(symbols)
                    qty = random.randint(1, 5)
                    price = get_price(symbol, current_date)

                    if price is None:
                        print(f"No price data available for {symbol} on {current_date}")
                        continue
                    else:
                        print(f"Price for {symbol} on {current_date}: {price}")

                    balance = get_balance(member_id)
                    print(f"Member {member_id} has balance: {balance}")

                    if balance is None:
                        print(f"No balance data available for member {member_id}")
                        continue

                    value = price * qty

                    # Initialize holdings for member and symbol if not already present
                    if member_id not in holdings:
                        holdings[member_id] = {}
                    if symbol not in holdings[member_id]:
                        holdings[member_id][symbol] = {'buy_qty': 0, 'sell_qty': 0}

                    if current_date < start_date + timedelta(days=3):
                        # Buy action
                        action = 'BUY'
                        balance = buyStock(balance, value)
                        if balance != 0:
                            update_bal(balance, member_id)
                        # Update holdings after buying
                        holdings[member_id][symbol]['buy_qty'] += qty

                    else:
                        action = random.choice(['BUY', 'SELL'])
                        if action == 'BUY':
                            balance = buyStock(balance, value)
                            if balance != 0:
                                update_bal(balance, member_id)
                            # Update holdings after buying
                            holdings[member_id][symbol]['buy_qty'] += qty
                        else:
                            # Sell action: ensure the member has enough stock to sell
                            available_qty = holdings[member_id][symbol]['buy_qty'] - holdings[member_id][symbol][
                                'sell_qty']

                            if available_qty > 0:
                                # Ensure sell qty does not exceed available qty
                                qty = min(random.randint(1, qty), available_qty)

                                current_price = get_price(symbol, current_date)
                                if current_price is None:
                                    print(f"No price data available for {symbol} on {current_date}")
                                else:
                                    current_value = current_price * qty
                                    balance = sellStock(balance, current_value)
                                    update_bal(balance, member_id)

                                    # Update holdings after selling
                                    holdings[member_id][symbol]['sell_qty'] += qty
                            else:
                                # Skip the sell action if no stock available
                                print(f"Member {member_id} has no {symbol} stock to sell.")

                    new_transaction = pd.DataFrame({
                        'MEMBER_ID': [member_id],
                        'SYMBOL': [symbol],
                        'action': [action],
                        'qty': [qty],
                        'date': [current_date.strftime('%Y-%m-%d')],
                        'remaining_qty': [None],  # Keeping None for columns without values
                        'CLOSE': [None],
                        'current_price': [None],
                        'total_buy_qty': [None],
                        'total_sell_qty': [None],
                        'total_investment': [None],
                        'total_sell': [None],
                        'average_investment': [None],
                        'current_investment': [None],
                        'profit': [None]
                    })

                    transactions = pd.concat([transactions, new_transaction], ignore_index=True)

                    print(f"Current Transactions DataFrame:\n{transactions}")

            # Write transactions to CSV and database
            if not transactions.empty:
                transactions.to_csv(r"C:\Users\Hp\PycharmProjects\STOCK\STOCK\FINALS\transactions.csv", index=False)
                transactions.to_sql('transactions', con=engine, if_exists='replace', index=False)

# Execute the function to make transactions
make_transactions()