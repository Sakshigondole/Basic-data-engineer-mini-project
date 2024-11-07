# Data Engineering Project: ETL Pipeline for Sales Data

import pandas as pd
import sqlite3
from datetime import datetime
import logging

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class SalesDataPipeline:
    def __init__(self, input_file, db_name):
        self.input_file = input_file
        self.db_name = db_name
        self.conn = None
    
    def extract_data(self):
        """Extract data from CSV file"""
        try:
            logger.info("Starting data extraction...")
            df = pd.read_csv(self.input_file)
            logger.info(f"Successfully extracted {len(df)} records")
            return df
        except Exception as e:
            logger.error(f"Error during extraction: {str(e)}")
            raise
    
    def transform_data(self, df):
        """Transform the data"""
        try:
            logger.info("Starting data transformation...")
            
            # Convert date strings to datetime
            df['date'] = pd.to_datetime(df['date'])
            
            # Calculate total sales
            df['total_amount'] = df['quantity'] * df['unit_price']
            
            # Remove duplicates
            df = df.drop_duplicates()
            
            # Handle missing values
            df = df.fillna(0)
            
            logger.info("Data transformation completed")
            return df
        except Exception as e:
            logger.error(f"Error during transformation: {str(e)}")
            raise
    
    def load_data(self, df):
        """Load data into SQLite database"""
        try:
            logger.info("Starting data loading...")
            self.conn = sqlite3.connect(self.db_name)
            
            # Create sales table if it doesn't exist
            create_table_query = """
            CREATE TABLE IF NOT EXISTS sales (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                date DATE,
                product_id INTEGER,
                quantity INTEGER,
                unit_price FLOAT,
                total_amount FLOAT
            )
            """
            self.conn.execute(create_table_query)
            
            # Load the data
            df.to_sql('sales', self.conn, if_exists='append', index=False)
            
            logger.info(f"Successfully loaded {len(df)} records to database")
        except Exception as e:
            logger.error(f"Error during loading: {str(e)}")
            raise
        finally:
            if self.conn:
                self.conn.close()
    
    def run_pipeline(self):
        """Execute the full ETL pipeline"""
        try:
            # Extract
            df = self.extract_data()
            
            # Transform
            df_transformed = self.transform_data(df)
            
            # Load
            self.load_data(df_transformed)
            
            logger.info("ETL pipeline completed successfully")
        except Exception as e:
            logger.error(f"Pipeline failed: {str(e)}")
            raise

if __name__ == "__main__":
    # Example usage
    input_file = "sales_data.csv"
    db_name = "sales_database.db"
    
    pipeline = SalesDataPipeline(input_file, db_name)
    pipeline.run_pipeline()
