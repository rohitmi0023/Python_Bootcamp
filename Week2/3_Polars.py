import polars as pl
from pathlib import Path

path = Path('mock_sales_data.csv')
path2 = Path('product_details.csv')

def read_data(path: Path, path2: Path) -> pl.DataFrame:
    try:
        df = pl.read_csv(path)
        print(f'Successfully parsed the csv file {path} with {df.shape[0]} rows and {df.shape[1]} columns')
        df2 = pl.read_csv(path2)
        print(f'Successfully parsed the csv file {path2} with {df2.shape[0]} rows and {df2.shape[1]} columns')
        return df, df2
    except FileNotFoundError as e:
        print(f'File not found. Please ensure the file exists in the specified path: {path}')
        return None
    except Exception as e:
        print(f'An error occurred while reading the file: {e}')
        return None
    

df, df2 = read_data(path, path2)

# polars df memory_usage
df_memory = df.estimated_size()

if df is not None and df2 is not None:
    print(f'Display first 5 rows: \n {df.head(5)}')
    print(f'Display data types: \n {df.dtypes}')
    print(f'Display summary statistics: \n {df.describe()}')

    # Filtering and Slicing
    bool_series = (df['Region'] == 'North') & (df['Category'] == 'Electronics')
    filtered_df = df.filter(bool_series)

    # groupby and aggregations
    grouped_df = df.group_by(['Region', 'Category']).agg(pl.col('Price').sum().alias('sum'), pl.col('Price').mean().alias('mean'))    

    # datetime
    df = df.with_columns(pl.col('OrderDate').str.to_datetime("%Y-%m-%d"))
    df = df.with_columns(pl.col('OrderDate').dt.month().alias('Month'), pl.col('OrderDate').dt.weekday().alias('WeekDay'))
    
    # monthly sales trend
    monthly_sales = df.group_by('Month').agg(pl.col('Price').sum().alias('MonthlySales')).sort('MonthlySales', descending=True)

    # sorting and ranking
    top_products = df.group_by('Product').agg(pl.col('Sales').sum().alias('TopProducts')).sort('TopProducts', descending=True).head(10)

    # merging and joining
    if df2 is not None:
        df_merged = df.join(df2, on='Product', how='inner')
        sales_by_manufacturer = df_merged.group_by('Manufacturer').agg(pl.col('Sales').sum())

    # Handling missing data
    missing_data_count = df.null_count()
    df_cleaned = df.drop_nulls(subset=['Price'])
    df_filled = df.with_columns(pl.col('Price').fill_null(df['Price'].mean()))

    # Creating new columns
    df = df.with_columns((0.1 * pl.col('Sales')).round(2).alias('Sales_Tax'))
    df = df.with_columns((pl.col('Sales') + pl.col('Sales_Tax')).round(2).alias('Final_Price'))