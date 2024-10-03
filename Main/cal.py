import datetime as dt
import mysql.connector

# Database connection
conn = mysql.connector.connect(
    host='localhost',
    user='root',
    password='test123',
    database='stock'
)

if conn.is_connected():
    print('Connected to the database')

def is_weekend(date):
    return 1 if date.weekday() >= 5 else 0  # 5=Saturday, 6=Sunday

def calculate_quarter(month):
    if month in [1, 2, 3]:
        return 1
    elif month in [4, 5, 6]:
        return 2
    elif month in [7, 8, 9]:
        return 3
    else:
        return 4

def create_calendar_table(start_date, end_date):
    cursor = conn.cursor()

    # Truncate the calendar table before inserting data
    cursor.execute('TRUNCATE TABLE calendar')

    insert_query = """INSERT INTO calendar(date, DayOfMonth, IsWeekend, WeekOfYear, Month, MonthNumber, Quarter, Year)
                      VALUES (%s, %s, %s, %s, %s, %s, %s, %s)"""

    current_date = start_date
    while current_date <= end_date:
        date = current_date
        day_of_month = date.day
        is_weekend_flag = is_weekend(date)
        week_of_year = date.isocalendar()[1]
        month = date.strftime("%B")
        month_number = date.month
        quarter = calculate_quarter(month_number)
        year = date.year

        # Record to insert
        record = (date, day_of_month, is_weekend_flag, week_of_year, month, month_number, quarter, year)
        cursor.execute(insert_query, record)

        current_date += dt.timedelta(days=1)

    conn.commit()
    cursor.close()

def main():
    start_date = dt.date(2023, 1, 1)
    end_date = dt.date(2024, 12, 31)
    create_calendar_table(start_date, end_date)

if __name__ == '__main__':
    main()

# Closing the connection after the table is created
conn.close()
