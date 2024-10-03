import yfinance as yf
import pandas as pd
import datetime as dt
import os

def Symbols():
    nse_symbols = pd.read_csv(r'C:\Users\Hp\PycharmProjects\STOCK\STOCK\FINALS\EQUITY_L.csv')
    lst_sym = nse_symbols['SYMBOL'].to_list()
    del lst_sym[20:]

    return lst_sym



def get_data(start_date,lst_sym):
    record = []
    for sym in lst_sym:
        stock_data = yf.Ticker(sym + '.NS').history(start=start_date,end=start_date + dt.timedelta(days=1))

        for date, row in stock_data.iterrows():
            record.append(
                [date.strftime("%Y-%m-%d"),
                 sym,
                 row['Open'],
                 row['High'],
                 row['Low'],
                 row['Close'],
                 row['Volume']
                 ]
            )
    # print(f"{sym} symbol {start_date},{end_date} is listed count is {count} ")
    df = pd.DataFrame(record, columns=['Date', 'Symbol', 'Open', 'High', 'Low', 'Close', 'Volume'])
    return df



def Single_day_stop(start_date: dt.date):
    symbols = Symbols()
    year,month,day = start_date.year,start_date.strftime('%b'),start_date.day
    os.makedirs(f"Y_M_D/{year}/{month}",exist_ok=True)
    df = get_data(start_date,symbols)
    if not df.empty:
        df.to_csv(f"Y_M_D/{year}/{month}/{start_date}.csv",index=False)


def main():
    start_date = dt.date.today()
    end_date = start_date + dt.timedelta(days=1)
    while start_date < end_date:
        Single_day_stop(start_date)
        print(f"->>>>>>>>>>>>>>>>>>{start_date}<<<<<<<<<<<<<<<<<<<<<<<<-")
        start_date+=dt.timedelta(days=1)


if __name__ == '__main__':
    main()








