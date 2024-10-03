from datetime import date
import subprocess


current_date = date.today()
print(current_date)


nse_daily = r"C:\Users\Admin\PycharmProjects\STOCK\FINALS\NSE_DATA_DAILY.py"

tran_daily = r"C:\Users\Admin\PycharmProjects\STOCK\FINALS\MOCK_TRAN_DAILY.py"

k = input(
    f"If you want to run {current_date} for stock data and append it to the SQL database, then press 'q' key: ")


if k == "q":
    try:

        subprocess.run(["python", nse_daily], check=True)
        print("nse Daily data generated...")
        print("appended to the SQL...")

        # Run the append to real CSV script
        subprocess.run(["python", tran_daily], check=True)
        print("transaction Daily data generated...")
        print("appended to the SQL...")

    except subprocess.CalledProcessError as e:
        print(f"An error occurred while running the scripts: {e}")


print("DONE ALL.............")