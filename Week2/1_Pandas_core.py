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

# %%
