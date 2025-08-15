# %%

import logging
import pandas as pd
from pathlib import Path
import numpy as np


logger = logging.getLogger(__name__)
logging.basicConfig(filename='memory_usage.log', level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')


def load_dataset_chunked(file_path, chunk_size=10_000):
    logger.debug(f'Started loading {chunk_size} chunks of {file_path} data')
    chunks=[]
    try:
        chunk_read_iterator = pd.read_csv(file_path,chunksize=chunk_size)
        chunks = []
        for i, chunk in enumerate(chunk_read_iterator):
            chunks.append(chunk)
            logger.debug(f'Loaded chunk {i} of shape {chunk.shape}')
        df = pd.concat(chunks, ignore_index=True)
        logger.info(f'Finished loading {file_path} into df head {df.head()}')
        return df
    except FileNotFoundError as e:
        logger.error(f'File not found: {file_path}')
        return None

path = Path('mock_sales_data.csv')
print(path.stat())
chunk_size = 10_000
# print(path.name)


#%%
# Memory Usage Profiling

def profile_memory_usage(df, description=''):
    memory_usage = df.memory_usage(deep=True)
    column_stats = []
    for col in df.columns:
        # print(col)
        col_memory = memory_usage[col]
        # print(col_memory)
        column_stats.append({
            'Column': col,
            'Memory (MB)': round(col_memory / (1024**2), 2),
            'Data Type': df[col].dtype,
            'Unique Values': df[col].nunique()
        })
        # sort by memory usage
    column_stats.sort(key=lambda x: x['Memory (MB)'], reverse=True)
    print(len(column_stats))
    for stat in column_stats:
        print(f"{stat['Column']:<15} | {str(stat['Data Type']):<12} | {stat['Memory (MB)']:>8} MB | {stat['Unique Values']:>8,} unique")

    print(f'Index Memory: {memory_usage.iloc[0]/1024**2:.2f} MB')


# %%
# Numerical Downcasting

def downcast_numerics(df):
    df_optimized = df.copy()
    numeric_columns = df_optimized.select_dtypes(include=['float64']).columns
    for col in numeric_columns:
        original_dtype = df_optimized[col].dtype
        original_memory = df_optimized[col].memory_usage(deep=True)
        # print(col, original_dtype, original_memory)
        downcasting_summary = {}

        # Downcast integers
        if pd.api.types.is_integer_dtype(df_optimized[col]):
            df_optimized[col] = pd.to_numeric(df_optimized[col], downcast='integer')
        
        # Downcast floats
        elif pd.api.types.is_float_dtype(df_optimized[col]):
            df_optimized[col] = pd.to_numeric(df_optimized[col], downcast='float')
        
        new_dtype = df_optimized[col].dtype
        new_memory = df_optimized[col].memory_usage(deep=True)
        memory_reduction = ((original_memory - new_memory) / original_memory) * 100
        
        downcasting_summary[col] = {
            'original_dtype': str(original_dtype),
            'new_dtype': str(new_dtype),
            'original_memory_mb': original_memory / (1024 * 1024),
            'new_memory_mb': new_memory / (1024 * 1024),
            'memory_reduction_percent': memory_reduction
        }
        
        print(f"{col:<15} | {str(original_dtype):<8} -> {str(new_dtype):<8} | "
              f"Reduction: {memory_reduction:>6.1f}%")

    total_original = sum([s['original_memory_mb'] for s in downcasting_summary.values()])
    total_new = sum([s['new_memory_mb'] for s in downcasting_summary.values()])
    total_reduction = ((total_original - total_new) / total_original) * 100

    print(f"\nTotal numeric memory reduction: {total_reduction:.1f}%")

    return df_optimized, downcasting_summary

# %%
if __name__ == '__main__':
    df = load_dataset_chunked(path, chunk_size)
    # profile_memory_usage(df)
    df_optimized = downcast_numerics(df)