import pandas as pd
from sqlalchemy import create_engine
path1 = r'C:\Users\Hp\PycharmProjects\STOCK\STOCK\target_symbols.csv'
path2 = r'C:\Users\Hp\PycharmProjects\STOCK\STOCK\FINALS\EQUITY_L.csv'
df1 = pd.read_csv(path1)
df2 = pd.read_csv(path2)

merged_df = pd.merge(df1, df2, on='SYMBOL', how='left')

engine = create_engine('mysql+pymysql://root:test123@localhost:3306/stock')
merged_df.to_sql('check_new_symbols', con=engine, if_exists='replace', index=False)
print('done')