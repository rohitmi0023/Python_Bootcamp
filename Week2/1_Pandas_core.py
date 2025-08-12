# %%
# Concepts

import pandas as pd

print(pd.__version__)

# %%
# creating raw data from scratch
data = {
    "name": ["Alice", "Bob", "Charlie"],
    "age": [25, 30, 35],
    "city": ["New York", "Los Angeles", "Chicago"]
}

df = pd.DataFrame(data)

print(df.head())


# %%
# Create a small sample DataFrame representing flight data
data2 = {
    'FlightNumber': ['AA100', 'BA200', 'CA300'],
    'Origin': ['JFK', 'LHR', 'PEK'],
    'Destination': ['LAX', 'SFO', 'HND'],
    'Departure': ['2025-08-11 08:30', '2025-08-12 13:45', '2025-08-13 22:10'],
    'Arrival': ['2025-08-11 11:30', '2025-08-12 16:30', '2025-08-14 06:00'],
    'Price': [320.00, 550.50, 410.75]
}

df2 = pd.DataFrame(data2)
print(df2)
df2.to_parquet('flights.parquet')

df3 = pd.read_parquet('Week2/flights.parquet')

# %%
# basics
pd.set_option('display.max_columns', 100)
df.head(10)
df.tail(10)
df.sample(10)
df.sample(frac=0.1) # 10% sample
df.columns # returns list of columns
df.index

# %%
# summary
df.info()
df.describe()
df[['name']].describe()
df.shape
len(df) # rows

# %%
# subsetting
df[['name', 'age']] # passing listing inside df
df[df.columns[:2]]
# can be used with list comprehensions as well

df.select_dtypes('object')


# %% 
# Filtering Rows

# based on rows
df.iloc[1]
type(df.iloc[1]) # Series
df.iloc[0,1]
df.iloc[0:3, :]
df.iloc[[1]] # filtering to row 1 as a dataframe
df.loc[:,['age']]
type(df.loc[:,'age']) # Series
type(df.loc[:,['age']]) # DataFrame

# boolean expressions
df.loc[df['age'] > 30]
df.query('(age > 30) and (city == "Chicago")')


# %%
# Groupby methods
df3.groupby('Origin')['Price'].sum()
df3.groupby('Origin')['Price'].agg(['sum','mean'])
df_agg = df3.groupby('Origin')[['Price','Price']].agg(['sum','mean'])
df_agg.columns 
# MultiIndex([('Price',  'sum'),
#             ('Price', 'mean'),
#             ('Price',  'sum'),
#             ('Price', 'mean')],
#            )

df_agg.columns = ['_'.join(col) for col in df_agg.columns]


# %% 
# Merge Data
df_m1 = df3.groupby(['Departure', 'Arrival'])[['Price']].mean()
df_m2 = df3.groupby(['Departure', 'Arrival'])[['Price']].sum()
df_merged = df_m1.merge(df_m2)
df_merged = df_m1.merge(df_m2, how='left')
pd.merge(df_m1, df_m2, how='left', suffixes=('_mean', '_sum'))
pd.merge(df_m1, df_m2, left_on='Departure', right_on='Departure')

# %%
# New Columns
df['PriceUSD'] = df['Price'] * 1.1
df = df.assign(PriceUSD = df['Price'] * 1.1)
# assign can be chained with other operations and returns a new object with all the old columns and the new ones