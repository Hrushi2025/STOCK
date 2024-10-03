import pymysql  # This helps us talk to the MySQL database
from STOCK.ConfigureFiles import config as cfg  # This is where we find our database details
from STOCK.ConfigureFiles import test_snapshot_config as ssc  # This is where we find our SQL questions

def execute_queries():
    cursor = None  # Initialize cursor to None
    connection = None  # Initialize connection to None

    try:
        # Try to connect to the database using pymysql
        connection = pymysql.connect(
            user=cfg.db_config['user'],  # Get the user name from our config
            password=cfg.db_config['password'],  # Get the password from our config
            host=cfg.db_config['host'],  # Get the address of the database from our config
            database=cfg.db_config['database']  # Get the name of the database from our config
        )
        cursor = connection.cursor()  # This will let us give commands to the database

        # Get the list of SQL questions from our snapShotConfig
        sql_queries = ssc.get_sql_queries()

        # For each SQL question in our list, do this
        for query in sql_queries:
            cursor.execute(query)  # Send the SQL question to the database
            connection.commit()  # Save the changes to the database
            print(f"Executed query: {query[:30]}...")  # Show a short part of the SQL question that was run

    except pymysql.MySQLError as e:
        # If something goes wrong, print the error message
        print(f"Error executing queries: {e}")

    finally:
        # No matter what happened, make sure to close everything
        if cursor:  # Check if cursor is initialized
            cursor.close()  # Stop talking to the database
        if connection:  # Check if connection is initialized
            connection.close()  # Close the connection to the database

# This part makes sure our function runs when we run this file
if __name__ == '__main__':
    execute_queries()
