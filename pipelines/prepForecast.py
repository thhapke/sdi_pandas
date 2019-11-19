
import pandas as pd

# setting display options for df
pd.set_option('display.max_rows', 500)
pd.set_option('display.max_columns', 500)
pd.set_option('display.width', 1000)
pd.set_option('display.max_colwidth', 15)


df = pd.read_csv("/Users/d051079/data/Addresses/opportunities_accounts.csv", sep=';',nrows=None)

df['unweightedRevenue'] = df['ExpectedRevenue']
df['weightedRevenue'] = df['ExpectedRevenue'] * df['Probability'] / 100
df['CloseDate'] = pd.to_datetime(df['CloseDate'])
df['Month'] = pd.DatetimeIndex(df['CloseDate']).month
df.loc[df['Month'] < 4,'Q1'] = df.loc[df['Month'] <4 ,'ExpectedRevenue']
df.loc[(df['Month'] >3) & (df['Month'] <7) ,'Q2'] = df.loc[(df['Month'] >3) & (df['Month'] <7),'ExpectedRevenue']
df.loc[(df['Month'] >6) & (df['Month'] <10),'Q3'] = df.loc[(df['Month'] >6) & (df['Month'] <10),'ExpectedRevenue']
df.loc[df['Month'] > 9,'Q4'] = df.loc[df['Month'] > 9,'ExpectedRevenue']

print(df)