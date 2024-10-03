import pymysql
from STOCK.ConfigureFiles import config as cfg
from STOCK.ConfigureFiles import snapShotConfig as ssc

def execute_queries():
    cursor = None
    connection = None

    try:
        connection = pymysql.connect(
            user=cfg.db_config['user'],
            password=cfg.db_config['password'],
            host=cfg.db_config['host'],
            database=cfg.db_config['database'],
            autocommit=False  # Disable autocommit for better performance
        )
        cursor = connection.cursor()

        # Fetch the maximum date from the trans table
        max_date_query = "SELECT MAX(date) AS max_date FROM trans"
        cursor.execute(max_date_query)
        max_date_result = cursor.fetchone()
        max_date = max_date_result[0] if max_date_result and max_date_result[0] else None

        # Fetch data from member_close_buy_sell based on the max date
        if max_date:
            fetch_query = """
                SELECT member_id, symbol, action, qty, date, close
                FROM member_close_buy_sell
                WHERE date > %s
            """
            cursor.execute(fetch_query, (max_date,))
        else:
            fetch_query = "SELECT member_id, symbol, action, qty, date, close FROM member_close_buy_sell"
            cursor.execute(fetch_query)

        results = cursor.fetchall()

        # Fetch existing records to avoid individual checks
        if max_date:
            existing_query = """
                SELECT MEMBER_ID, SYMBOL, action, date
                FROM trans
                WHERE date > %s
            """
            cursor.execute(existing_query, (max_date,))
        else:
            existing_query = "SELECT MEMBER_ID, SYMBOL, action, date FROM trans"
            cursor.execute(existing_query)

        existing_records = set(cursor.fetchall())

        # Prepare data for bulk insert
        new_records = []
        for row in results:
            if len(row) == 6:
                member_id, symbol, action, qty, date, close = row
                if (member_id, symbol, action, date) not in existing_records:
                    new_records.append((member_id, symbol, action, qty, date, close))
            else:
                continue  # Skip rows with incorrect number of values

        # Bulk insert new records
        if new_records:
            insert_query = """
                INSERT INTO trans (
                    MEMBER_ID, SYMBOL, action, qty, date, close, 
                    remaining_qty, current_price, total_buy_qty, 
                    total_sell_qty, total_investment, total_sell, 
                    average_price, current_investment, profit, 
                    avg_investment, avg_current_investment, invested_amt, Nprofit
                ) VALUES (
                    %s, %s, %s, %s, %s, %s, 
                    NULL, NULL, NULL, NULL, NULL, NULL, 
                    NULL, NULL, NULL, NULL, NULL, NULL, NULL
                )
            """
            cursor.executemany(insert_query, new_records)
            inserted_rows = cursor.rowcount
            connection.commit()
            print(f"Inserted {inserted_rows} new rows into the trans table.")
        else:
            print("No new records to insert.")

        # Execute SQL questions from snapShotConfig
        sql_queries = ssc.get_sql_queries()
        for query in sql_queries:
            cursor.execute(query)
            connection.commit()
            print(f"Executed query: {query[:30]}...")

    except pymysql.MySQLError as e:
        print(f"Error executing queries: {e}")
        if connection:
            connection.rollback()  # Rollback in case of error
    finally:
        if cursor:
            cursor.close()
        if connection:
            connection.close()

if __name__ == '__main__':
    execute_queries()
