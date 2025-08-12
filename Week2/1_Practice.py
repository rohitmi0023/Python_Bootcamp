# %%
import pandas as pd
from pathlib import Path

path = Path('mock_sales_data.csv')
path2 = Path('product_details.csv')

def read_data(path: Path) -> pd.DataFrame:
    try:
        df = pd.read_csv(path)
        print(f'Successfully parsed the csv file {path} with {df.shape[0]} rows and {df.shape[1]} columns')
        df2 = pd.read_csv(path2)
        print(f'Successfully parsed the csv file {path2} with {df2.shape[0]} rows and {df2.shape[1]} columns')
        return df
    except FileNotFoundError as e:
        print(f'File not found. Please ensure the file exists in the specified path: {path}')
        return None
    except Exception as e:
        print(f'An error occurred while reading the file: {e}')
        return None

df = read_data(path)

# Load and Inspect Data
if df is not None:
    print(f'Display first 5 rows: \n {df.head(5)}')
    print(f'Display data types: \n {df.dtypes}')
    print(f'Display summary statistics: \n {df.describe()}')

# Filtering and Slicing
    bool_series = (df['Region'] == 'North') & (df['Category'] == 'Electronics')
    filtered_df = df[bool_series]

# groupby and aggregations
    grouped_df = df.groupby(['Region', 'Category'])['Price'].agg(['sum', 'mean'])


# datetime
    df['OrderDate'] = pd.to_datetime(df['OrderDate'])
    df['Month'] = df['OrderDate'].dt.month
    df['WeekDay'] = df['OrderDate'].dt.weekday
    # monthly sales trend
    df.groupby(df['Month'])['Sales'].sum().sort_values(ascending=False)

# sorting and ranking
    df.groupby(['Product'])['Sales'].sum().sort_values(ascending=10).head(10)

# merging and joining
    if df2 is not None:
        df_merged = pd.merge(df, df2, how='inner', on='Product')
        df_merged.groupby(['Manufacturer'])['Sales'].sum()

# Handling missing data
    






# sample source data generation code below

# %%
# Mock Sales Dataset
import pandas as pd
import numpy as np

# Set seed for reproducibility
np.random.seed(42)

# Create sample regions, categories, products
regions = ["North", "South", "East", "West"]
categories = ["Electronics", "Furniture", "Clothing", "Food"]
products = {
    "Electronics": ["Laptop", "Smartphone", "TV", "Headphones", "Camera"],
    "Furniture": ["Chair", "Table", "Sofa", "Bed", "Desk"],
    "Clothing": ["Shirt", "Pants", "Jacket", "Shoes", "Dress"],
    "Food": ["Bread", "Milk", "Eggs", "Chicken", "Rice"]
}

# Generate 50K rows
n_rows = 50000

# Random selection
region_data = np.random.choice(regions, size=n_rows)
category_data = np.random.choice(categories, size=n_rows)

product_data = [
    np.random.choice(products[cat]) for cat in category_data
]

# Dates in 2023, monthly distribution
dates = pd.date_range(start="2023-01-01", end="2023-12-31", freq="D")
date_data = np.random.choice(dates, size=n_rows)

# Prices and quantities
price_data = np.round(np.random.uniform(5, 2000, size=n_rows), 2)
quantity_data = np.random.randint(1, 6, size=n_rows)

# Create DataFrame
sales_df = pd.DataFrame({
    "OrderID": np.arange(1, n_rows+1),
    "OrderDate": date_data,
    "Region": region_data,
    "Category": category_data,
    "Product": product_data,
    "Price": price_data,
    "Quantity": quantity_data
})

# Add a Sales column
sales_df["Sales"] = sales_df["Price"] * sales_df["Quantity"]

# Save to CSV
sales_df.to_csv("mock_sales_data.csv", index=False)

print(sales_df.head())

# %%
# Create unique product listing
product_list = []
for cat, items in products.items():
    for p in items:
        product_list.append((p, cat))

product_details_df = pd.DataFrame(product_list, columns=["Product", "Category"])
product_details_df["ProductID"] = range(1001, 1001 + len(product_details_df))
product_details_df["Manufacturer"] = np.random.choice(
    ["CompanyA", "CompanyB", "CompanyC", "CompanyD"], size=len(product_details_df)
)
product_details_df["WarrantyMonths"] = np.random.choice([6, 12, 24, 36], size=len(product_details_df))

# Save to CSV
product_details_df.to_csv("product_details.csv", index=False)

print(product_details_df)
