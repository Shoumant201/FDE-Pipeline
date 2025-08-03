"""
Database_Connector.py
Handles database connections and data loading operations
"""

import psycopg2
import pandas as pd
import logging
from sqlalchemy import create_engine
from string import Template

# Configure logging system with basic setup - INFO level and above
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
# Create logger object specific to this module
logger = logging.getLogger(__name__)

class DatabaseConnector:
    def __init__(self, db_config):
        self.db_config = db_config
        self.connection = None
        self.engine = None
        
    def get_connection(self):
        """
        Provide psycopg2 connection for executing raw and complex SQL queries
        """
        try:
            if not self.connection:
                self.connection = psycopg2.connect(
                    host=self.db_config['host'],
                    database=self.db_config['database'],
                    user=self.db_config['user'],
                    password=self.db_config['password'],
                    port=self.db_config['port']
                )
                logger.info("psycopg2 connection established successfully")
            
            return self.connection
            
        except Exception as e:
            logger.error(f"Failed to establish psycopg2 connection: {str(e)}")
            return None
    
    def get_engine(self):
        """
        Provide SQLAlchemy engine connection for ORM operations and high-level queries
        """
        try:
            if not self.engine:
                # Create connection string template for SQLAlchemy
                connection_template = Template("postgresql://$user:$password@$host:$port/$database")
                
                # Substitute template variables with actual config values
                connection_string = connection_template.substitute(
                    user=self.db_config['user'],
                    password=self.db_config['password'],
                    host=self.db_config['host'],
                    port=self.db_config['port'],
                    database=self.db_config['database']
                )
                
                # Create SQLAlchemy engine
                self.engine = create_engine(connection_string)
                logger.info("SQLAlchemy engine created successfully")
            
            return self.engine
            
        except Exception as e:
            logger.error(f"Failed to create SQLAlchemy engine: {str(e)}")
            return None
        
    def load_to_landing_table(self, data, table_name):
        """
        Load processed data into landing table
        """
        try:
            # Truncate existing data in landing table
            self.truncate_table(table_name)
            
            # Convert data to DataFrame if it's not already
            if not isinstance(data, pd.DataFrame):
                df = pd.DataFrame(data)
            else:
                df = data
            
            # Get SQLAlchemy engine for data loading
            engine = self.get_engine()
            if not engine:
                raise Exception("Failed to get SQLAlchemy engine")
            
            # Load new data using SQLAlchemy engine
            df.to_sql(table_name, engine, if_exists='append', index=False)
            
            logger.info(f"Successfully loaded {len(df)} records to {table_name}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to load data to {table_name}: {str(e)}")
            return False
        
    def truncate_table(self, table_name):
        """
        Truncate specified landing table using raw SQL
        """
        try:
            # Get psycopg2 connection for raw SQL operations
            connection = self.get_connection()
            if not connection:
                raise Exception("Failed to get database connection")
            
            cursor = connection.cursor()
            cursor.execute(f"TRUNCATE TABLE {table_name}")
            connection.commit()
            cursor.close()
            
            logger.info(f"Successfully truncated table {table_name}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to truncate table {table_name}: {str(e)}")
            if self.connection:
                self.connection.rollback()
            return False
        
    def close_connection(self):
        """
        Close database connections
        """
        try:
            if self.connection:
                self.connection.close()
                logger.info("psycopg2 connection closed")
            
            if self.engine:
                self.engine.dispose()
                logger.info("SQLAlchemy engine disposed")
                
        except Exception as e:
            logger.error(f"Error closing connections: {str(e)}")

if __name__ == "__main__":
    # Test database connection
    import yaml
    import os
    from dotenv import load_dotenv
    
    # Load environment variables
    load_dotenv()
    
    # Load config
    with open('config.yaml', 'r') as file:
        config = yaml.safe_load(file)
    
    # Substitute environment variables
    db_config = {
        'host': os.getenv('DB_HOST'),
        'database': config['database']['database'],
        'user': os.getenv('DB_USER'),
        'password': os.getenv('DB_PASSWORD'),
        'port': os.getenv('DB_PORT')
    }
    
    # Test connections
    db_connector = DatabaseConnector(db_config)
    
    # Test psycopg2 connection
    connection = db_connector.get_connection()
    if connection:
        logger.info("psycopg2 connection test successful!")
    else:
        logger.error("psycopg2 connection test failed!")
    
    # Test SQLAlchemy engine
    engine = db_connector.get_engine()
    if engine:
        logger.info("SQLAlchemy engine test successful!")
    else:
        logger.error("SQLAlchemy engine test failed!")
    
    # Close connections
    db_connector.close_connection()