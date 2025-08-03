"""
CSV_Extractor.py
Handles processing and extraction of CSV format data
"""

import pandas as pd
import logging
from sqlalchemy.sql import text
from datetime import datetime
import re

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class CSVExtractor:
    def __init__(self, db_connector):
        self.db_connector = db_connector
        
    @staticmethod
    def camel_to_snake(name):
        """
        Convert camelCase or inconsistent column names to snake_case
        """
        # Replace spaces and dashes with underscores
        name = re.sub(r'[\s\-]+', '_', name)
        # Convert camelCase to snake_case
        name = re.sub(r'([a-z0-9])([A-Z])', r'\1_\2', name)
        # Convert to lowercase and remove extra underscores
        name = re.sub(r'_+', '_', name.lower().strip('_'))
        return name
        
    def load_to_landing(self, df, table_name, source_file=None):
        """
        Main method to load CSV data to landing table
        """
        try:
            # Get table columns from database
            table_columns = self.get_table_columns(table_name)
            
            # Check if table uses raw_data column (flexible approach)
            if 'raw_data' in table_columns:
                # Store CSV data as JSON records
                records = df.to_dict('records')
                
                # Use JSON extractor for consistent handling
                from JSON_Extractor import JSONExtractor
                json_extractor = JSONExtractor(self.db_connector)
                success = json_extractor.load_to_landing(records, table_name, file_name=source_file)
                
            else:
                # Traditional structured approach
                # Normalize column names to snake_case
                df.columns = [self.camel_to_snake(col) for col in df.columns]
                
                # Add metadata fields if they exist in table
                if 'loaded_at' in table_columns:
                    df['loaded_at'] = datetime.now()
                
                if 'source_file' in table_columns and source_file:
                    df['source_file'] = source_file
                
                # Filter DataFrame to keep only matching columns
                matching_columns = [col for col in df.columns if col in table_columns]
                df_filtered = df[matching_columns]
                
                # Load data to landing table using the same connection approach
                try:
                    # Use the same connection that we used to check columns
                    engine = self.db_connector.get_engine()
                    if not engine:
                        raise Exception("Failed to get SQLAlchemy engine")
                    
                    # Truncate table first
                    connection = self.db_connector.get_connection()
                    cursor = connection.cursor()
                    cursor.execute(f"TRUNCATE TABLE {table_name}")
                    connection.commit()
                    cursor.close()
                    
                    # Load data using pandas to_sql with proper schema handling
                    if '.' in table_name:
                        schema_name, table_name_only = table_name.split('.', 1)
                        df_filtered.to_sql(table_name_only, engine, 
                                         schema=schema_name,
                                         if_exists='append', index=False)
                    else:
                        df_filtered.to_sql(table_name, engine, 
                                         if_exists='append', index=False)
                    
                    logger.info(f"Successfully loaded {len(df_filtered)} records to {table_name}")
                    success = True
                    
                except Exception as e:
                    logger.error(f"Failed to load data to {table_name}: {str(e)}")
                    success = False
            
            if success:
                logger.info(f"Successfully loaded {len(df)} records to {table_name}")
            else:
                logger.error(f"Failed to load data to {table_name}")
                
            return success
            
        except Exception as e:
            logger.error(f"Error in load_to_landing: {str(e)}")
            return False
    
    def get_table_columns(self, table_name):
        """
        Get column names from database table (handles schema.table format)
        """
        try:
            connection = self.db_connector.get_connection()
            cursor = connection.cursor()
            
            # Handle schema.table format
            if '.' in table_name:
                schema_name, table_name_only = table_name.split('.', 1)
                query = """
                SELECT column_name 
                FROM information_schema.columns 
                WHERE table_schema = %s AND table_name = %s
                ORDER BY ordinal_position
                """
                cursor.execute(query, (schema_name, table_name_only))
                logger.info(f"Querying columns for schema: {schema_name}, table: {table_name_only}")
            else:
                query = """
                SELECT column_name 
                FROM information_schema.columns 
                WHERE table_name = %s
                ORDER BY ordinal_position
                """
                cursor.execute(query, (table_name,))
                logger.info(f"Querying columns for table: {table_name}")
            
            columns = [row[0] for row in cursor.fetchall()]
            cursor.close()
            
            logger.info(f"Found columns for {table_name}: {columns}")
            return columns
            
        except Exception as e:
            logger.error(f"Error getting table columns for {table_name}: {str(e)}")
            return []
        
    def extract_from_file(self, file_path, table_name):
        """
        Extract data from CSV file
        """
        try:
            df = pd.read_csv(file_path)
            return self.load_to_landing(df, table_name, source_file=file_path)
            
        except Exception as e:
            logger.error(f"Error extracting from file {file_path}: {str(e)}")
            return False
        
    def extract_from_dataframe(self, df, table_name, source_file=None):
        """
        Extract data from pandas DataFrame (for S3 objects)
        """
        try:
            return self.load_to_landing(df, table_name, source_file=source_file)
            
        except Exception as e:
            logger.error(f"Error extracting from DataFrame: {str(e)}")
            return False

if __name__ == "__main__":
    # Test CSV extraction
    from Database_Connector import DatabaseConnector
    import yaml
    import os
    from dotenv import load_dotenv
    
    # Load environment variables and config
    load_dotenv()
    
    with open('config.yaml', 'r') as file:
        config = yaml.safe_load(file)
    
    db_config = {
        'host': os.getenv('DB_HOST'),
        'database': config['database']['database'],
        'user': os.getenv('DB_USER'),
        'password': os.getenv('DB_PASSWORD'),
        'port': os.getenv('DB_PORT')
    }
    
    # Test CSV extractor
    db_connector = DatabaseConnector(db_config)
    csv_extractor = CSVExtractor(db_connector)
    
    logger.info("CSV Extractor initialized successfully")