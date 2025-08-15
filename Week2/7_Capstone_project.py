#!/usr/bin/env python3
"""
Multi-Format Data Ingestion & Transformation Pipeline
====================================================
Interview-ready capstone project demonstrating:
- Multi-format data ingestion (CSV, JSON, Parquet)
- Data type optimization and memory management
- Cross-dataset joins and aggregations
- Mixed library usage (Pandas, Polars, NumPy)
- Professional logging and error handling
"""

import pandas as pd
import polars as pl
import numpy as np
import json
import logging
from pathlib import Path
import time
import psutil
from typing import Dict, List
from datetime import datetime, timedelta
import random

# Configure logging
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('Week2/pipeline.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class DataPipeline:
    """
    Multi-format data ingestion and transformation pipeline
    Handles CSV, JSON, and Parquet files with memory optimization
    """
    
    def __init__(self, raw_data_path: str = "raw_data", processed_path: str = "processed"):
        self.raw_data_path = Path(raw_data_path)
        self.processed_path = Path(processed_path)
        self.memory_logs: List[dict] = []
        
        # Create directories if they don't exist
        self.raw_data_path.mkdir(exist_ok=True)
        self.processed_path.mkdir(exist_ok=True)
        
        logger.info(f"Pipeline initialized with raw_data: {raw_data_path}, processed: {processed_path}")
    
    def log_memory_usage(self, step: str) -> None:
        """Log current memory usage"""
        memory_mb = psutil.Process().memory_info().rss / 1024 / 1024
        self.memory_logs.append({
            'step': step,
            'memory_mb': round(memory_mb, 2),
            'timestamp': datetime.now()
        })
        logger.info(f"Memory usage at {step}: {memory_mb:.2f} MB at {self.memory_logs[-1]['timestamp']}")
    
    def optimize_dtypes(self, df: pd.DataFrame) -> pd.DataFrame:
        """Optimize DataFrame data types for memory efficiency"""
        logger.info("Optimizing data types...")
        
        try:
            df_opt = df.infer_objects().convert_dtypes()
            
            for col in df_opt.select_dtypes(include=['string', 'object']):
                if df_opt[col].nunique() / len(df_opt) < 0.5:
                    df_opt[col] = df_opt[col].astype('category')
                    logger.info(f"Converted {col} to category (cardinality: {df_opt[col].nunique()}/{len(df_opt)})")
            
            return df_opt
            
        except Exception as e:
            logger.warning(f"Dtype optimization failed: {e}, returning original DataFrame")
            return df
    
    def ingest_csv_data(self) -> Dict[str, pd.DataFrame]:
        """Ingest all CSV files using Pandas"""
        logger.info("Ingesting CSV files...")
        csv_data = {}
        
        for csv_file in self.raw_data_path.glob("*.csv"):
            try:
                df = pd.read_csv(csv_file)
                df = self.optimize_dtypes(df)
                csv_data[csv_file.stem] = df
                logger.info(f"Loaded {csv_file.name}: {df.shape[0]} rows, {df.shape[1]} columns")
            except Exception as e:
                logger.error(f"Error loading {csv_file.name}: {e}")
        
        self.log_memory_usage("CSV ingestion")
        return csv_data
    
    def ingest_json_data(self) -> Dict[str, pd.DataFrame]:
        """Ingest all JSON files using Pandas"""
        logger.info("Ingesting JSON files...")
        json_data = {}
        
        for json_file in self.raw_data_path.glob("*.json"):
            try:
                df = pd.read_json(json_file)
                df = self.optimize_dtypes(df)
                json_data[json_file.stem] = df
                logger.info(f"Loaded {json_file.name}: {df.shape[0]} rows, {df.shape[1]} columns")
            except Exception as e:
                logger.error(f"Error loading {json_file.name}: {e}")
        
        self.log_memory_usage("JSON ingestion")
        return json_data
    
    def ingest_parquet_data(self) -> Dict[str, pd.DataFrame]:
        """Ingest all Parquet files using Pandas"""
        logger.info("Ingesting Parquet files...")
        parquet_data = {}
        
        for parquet_file in self.raw_data_path.glob("*.parquet"):
            try:
                df = pd.read_parquet(parquet_file)
                parquet_data[parquet_file.stem] = df
                logger.info(f"Loaded {parquet_file.name}: {df.shape[0]} rows, {df.shape[1]} columns")
            except Exception as e:
                logger.error(f"Error loading {parquet_file.name}: {e}")
        
        self.log_memory_usage("Parquet ingestion")
        return parquet_data
    
    def perform_joins(self, datasets: Dict[str, pd.DataFrame]) -> pd.DataFrame:
        """Join datasets into unified DataFrame"""
        logger.info("Performing cross-dataset joins...")
        logger.debug(f'unified dataset products_data shape {datasets['products_data'].shape}, sales_data shape {datasets['sales_data'].shape}')
        # Start with the largest dataset as base
        base_name = max(datasets.keys(), key=lambda k: len(datasets[k]))
        unified_df = datasets[base_name].copy()
        logger.debug(f"Using {base_name} as base dataset with {len(unified_df)} rows")
        
        # Join other datasets
        for name, df in datasets.items():
            if name == base_name:
                continue
            
            # Find common columns for joining
            common_cols = set(unified_df.columns).intersection(set(df.columns))
            if common_cols:
                join_col = list(common_cols)[0]  # Use first common column
                unified_df = unified_df.merge(
                    df, 
                    on=join_col, 
                    how='left', 
                    suffixes=('', f'_{name}')
                )
                logger.info(f"Joined {name} on column '{join_col}'")
            else:
                logger.warning(f"No common columns found for {name}, skipping join")
        
        self.log_memory_usage("Data joins")
        return unified_df
    
    def calculate_kpis_numpy(self, df: pd.DataFrame) -> Dict[str, float]:
        """Calculate KPIs using NumPy for performance"""
        logger.info("Calculating KPIs with NumPy...")
        
        kpis = {}
        
        # Find numeric columns
        numeric_cols = df.select_dtypes(include=[np.number]).columns
        
        for col in numeric_cols:
            values = df[col].dropna().values
            if len(values) > 0:
                kpis[f'{col}_mean'] = np.mean(values)
                kpis[f'{col}_std'] = np.std(values)
                kpis[f'{col}_median'] = np.median(values)
                kpis[f'{col}_95th_percentile'] = np.percentile(values, 95)
        
        return kpis
    
    def aggregate_with_polars(self, df: pd.DataFrame) -> Dict[str, pl.DataFrame]:
        """Perform large aggregations using Polars"""
        logger.info("Performing aggregations with Polars...")
        
        # Convert to Polars
        pl_df = pl.from_pandas(df)
        aggregations = {}
        
        # Find date column for time-based aggregations
        date_cols = [col for col in df.columns if df[col].dtype == 'datetime64[ns]']
        numeric_cols = [col for col in df.columns if df[col].dtype in ['int64', 'float64', 'int32', 'float32']]
        
        if date_cols and numeric_cols:
            date_col = date_cols[0]
            
            try:
                # Monthly aggregations
                monthly_agg = (
                    pl_df
                    .with_columns(pl.col(date_col).dt.truncate("1mo").alias("month"))
                    .group_by("month")
                    .agg([
                        pl.col(col).mean().alias(f"{col}_avg") for col in numeric_cols[:3]  # Limit to first 3 numeric cols
                    ] + [
                        pl.col(col).sum().alias(f"{col}_sum") for col in numeric_cols[:3]
                    ])
                )
                aggregations['monthly'] = monthly_agg
                
                # Quarterly aggregations
                quarterly_agg = (
                    pl_df
                    .with_columns(pl.col(date_col).dt.quarter().alias("quarter"))
                    .group_by("quarter")
                    .agg([
                        pl.col(col).mean().alias(f"{col}_avg") for col in numeric_cols[:3]
                    ])
                )
                aggregations['quarterly'] = quarterly_agg
                
                logger.info("Completed time-based aggregations")
            
            except Exception as e:
                logger.warning(f"Could not perform time-based aggregations: {e}")
        
        self.log_memory_usage("Polars aggregations")
        return aggregations
    
    def save_results(self, unified_df: pd.DataFrame, aggregations: Dict, kpis: Dict) -> None:
        """Save processed results to files"""
        logger.info("Saving processed results...")
        
        # Save unified DataFrame
        unified_df.to_parquet(self.processed_path / "unified_data.parquet", index=False)
        unified_df.to_csv(self.processed_path / "unified_data.csv", index=False)
        
        # Save aggregations
        for agg_name, agg_df in aggregations.items():
            if isinstance(agg_df, pl.DataFrame):
                agg_df.write_parquet(self.processed_path / f"{agg_name}_aggregation.parquet")
                agg_df.write_csv(self.processed_path / f"{agg_name}_aggregation.csv")
        
        # Save KPIs
        with open(self.processed_path / "kpis.json", 'w') as f:
            json.dump(kpis, f, indent=2, default=str)
        
        # Save memory logs
        memory_df = pd.DataFrame(self.memory_logs)
        memory_df.to_csv(self.processed_path / "memory_usage_log.csv", index=False)
        
        logger.info(f"Results saved to {self.processed_path}")
    
    def run_pipeline(self) -> None:
        """Execute the complete data pipeline"""
        start_time = time.time()
        logger.info("Starting data pipeline execution...")
        
        try:
            # Ingest all data formats
            json_data = self.ingest_json_data()
            parquet_data = self.ingest_parquet_data()
            csv_data = self.ingest_csv_data()
            
            # Combine all datasets
            all_datasets = {**csv_data, **json_data, **parquet_data}
            
            if not all_datasets:
                logger.error("No data files found! Please check the raw_data directory.")
                return
            
            # Create unified DataFrame
            unified_df = self.perform_joins(all_datasets)
            
            # Calculate KPIs with NumPy
            kpis = self.calculate_kpis_numpy(unified_df)
            
            # Perform aggregations with Polars
            aggregations = self.aggregate_with_polars(unified_df)
            
            # Save all results
            self.save_results(unified_df, aggregations, kpis)
            
            # Final statistics
            execution_time = time.time() - start_time
            logger.info(f"Pipeline completed successfully in {execution_time:.2f} seconds")
            logger.info(f"Processed {len(unified_df)} total rows across {len(all_datasets)} datasets")
            
        except Exception as e:
            logger.error(f"Pipeline failed: {e}")
            raise

# Sample Data Generator
class SampleDataGenerator:
    """Generate realistic sample datasets for the pipeline"""

    def __init__(self, raw_data_path: str = "raw_data"):
        self.raw_data_path = Path(raw_data_path)
        self.raw_data_path.mkdir(exist_ok=True)
    
    def generate_sales_csv(self) -> None:
        """Generate sales data CSV"""
        logger.info(f"Generating sales_data.csv in {self.raw_data_path}...")
        
        # Generate 10,000 sales records
        data = []
        start_date = datetime(2023, 1, 1)
        
        for i in range(10000):
            data.append({
                'transaction_id': f'TXN_{i:06d}',
                'customer_id': f'CUST_{random.randint(1, 2000):04d}',
                'product_id': f'PROD_{random.randint(1, 500):03d}',
                'sale_date': start_date + timedelta(days=random.randint(0, 365)),
                'quantity': random.randint(1, 10),
                'unit_price': round(random.uniform(10, 500), 2),
                'total_amount': 0,  # Will calculate
                'region': random.choice(['North', 'South', 'East', 'West']),
                'sales_rep': f'REP_{random.randint(1, 50):02d}'
            })
        
        # Calculate total amount
        for record in data:
            record['total_amount'] = round(float(record['quantity']) * float(record['unit_price']), 2)
        
        df = pd.DataFrame(data)
        df.to_csv(self.raw_data_path / "sales_data.csv", index=False)
        logger.info(f"Generated sales_data.csv with {len(data)} records")
    
    def generate_customers_json(self) -> None:
        """Generate customer data JSON"""
        logger.info("Generating customers_data.json...")
        
        customers = []
        for i in range(2000):
            customers.append({
                'customer_id': f'CUST_{i+1:04d}',
                'first_name': random.choice(['John', 'Jane', 'Bob', 'Alice', 'Charlie', 'Diana']),
                'last_name': random.choice(['Smith', 'Johnson', 'Williams', 'Brown', 'Jones', 'Garcia']),
                'email': f'customer{i+1}@email.com',
                'age': random.randint(18, 80),
                'registration_date': (datetime(2020, 1, 1) + timedelta(days=random.randint(0, 1095))).isoformat(),
                'customer_segment': random.choice(['Premium', 'Standard', 'Basic']),
                'annual_spend': round(random.uniform(100, 5000), 2)
            })
        
        with open(self.raw_data_path / "customers_data.json", 'w') as f:
            json.dump(customers, f, indent=2)
        logger.info(f"Generated customers_data.json with {len(customers)} records")
    
    def generate_products_parquet(self) -> None:
        """Generate product data Parquet"""
        logger.info("Generating products_data.parquet...")
        
        products = []
        categories = ['Electronics', 'Clothing', 'Home', 'Sports', 'Books']
        
        for i in range(500):
            products.append({
                'product_id': f'PROD_{i+1:03d}',
                'product_name': f'Product {i+1}',
                'category': random.choice(categories),
                'cost': round(random.uniform(5, 200), 2),
                'price': round(random.uniform(10, 500), 2),
                'stock_quantity': random.randint(0, 1000),
                'supplier_id': f'SUP_{random.randint(1, 20):02d}',
                'launch_date': datetime(2020, 1, 1) + timedelta(days=random.randint(0, 1095))
            })
        
        df = pd.DataFrame(products)
        df.to_parquet(self.raw_data_path / "products_data.parquet", index=False)
        logger.info(f"Generated products_data.parquet with {len(products)} records")
    
    def generate_all_sample_data(self) -> None:
        """Generate all sample datasets"""
        logger.info("Generating all sample datasets...")
        self.generate_sales_csv()
        self.generate_customers_json()
        self.generate_products_parquet()
        logger.info("Sample data generation completed!")

def main():
    """Main execution function"""
    print("🧱 Multi-Format Data Pipeline - Interview Capstone Project")
    print("=" * 60)
    
    # Generate sample data
    generator = SampleDataGenerator("Week2/raw_data")
    generator.generate_all_sample_data()
    
    # Run the pipeline
    pipeline = DataPipeline("Week2/raw_data", "Week2/processed")
    pipeline.run_pipeline()
    
    print("\n✅ Pipeline execution completed!")
    print("Check the 'processed/' directory for results:")
    print("  - unified_data.parquet/csv (joined datasets)")
    print("  - *_aggregation.parquet/csv (time-based aggregations)")
    print("  - kpis.json (NumPy-calculated metrics)")
    print("  - memory_usage_log.csv (performance monitoring)")

if __name__ == "__main__":
    main()