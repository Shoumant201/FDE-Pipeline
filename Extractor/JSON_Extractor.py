"""
JSON_Extractor.py
Handles processing and extraction of JSON format data
"""

import json
import logging
from datetime import datetime
from psycopg2.extras import Json

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class JSONExtractor:
    def __init__(self, db_connector):
        self.db_connector = db_connector
        
    def load_to_landing(self, json_data, table_name, file_name=None, api_endpoint=None, response_status=None):
        """
        Main method to load JSON data directly into database with raw_data column
        """
        try:
            # Ensure json_data is a list for consistent processing
            if not isinstance(json_data, list):
                json_data = [json_data]
            
            # Get database connection
            connection = self.db_connector.get_connection()
            cursor = connection.cursor()
            
            # Get table columns to check what metadata fields exist
            table_columns = self.get_table_columns(table_name)
            
            # Build dynamic insert query based on available columns
            columns = ['raw_data']
            placeholders = ['%s']
            
            if 'loaded_at' in table_columns:
                columns.append('loaded_at')
                placeholders.append('%s')
                
            if 'file_name' in table_columns and file_name:
                columns.append('file_name')
                placeholders.append('%s')
            
            if 'api_endpoint' in table_columns and api_endpoint:
                columns.append('api_endpoint')
                placeholders.append('%s')
                
            if 'request_timestamp' in table_columns:
                columns.append('request_timestamp')
                placeholders.append('%s')
                
            if 'response_status' in table_columns and response_status is not None:
                columns.append('response_status')
                placeholders.append('%s')
            
            # Construct the insert query
            query = f"""
            INSERT INTO {table_name} ({', '.join(columns)}) 
            VALUES ({', '.join(placeholders)})
            """
            
            # Prepare data for insertion
            records_inserted = 0
            request_time = datetime.now()
            
            for json_obj in json_data:
                values = [Json(json_obj)]  # Use psycopg2.extras.Json for PostgreSQL JSON type
                
                if 'loaded_at' in table_columns:
                    values.append(datetime.now())
                    
                if 'file_name' in table_columns and file_name:
                    values.append(file_name)
                    
                if 'api_endpoint' in table_columns and api_endpoint:
                    values.append(api_endpoint)
                    
                if 'request_timestamp' in table_columns:
                    values.append(request_time)
                    
                if 'response_status' in table_columns and response_status is not None:
                    values.append(response_status)
                
                cursor.execute(query, values)
                records_inserted += 1
            
            # Commit the transaction
            connection.commit()
            cursor.close()
            
            logger.info(f"Successfully loaded {records_inserted} JSON records to {table_name}")
            return True
            
        except Exception as e:
            logger.error(f"Error in load_to_landing: {str(e)}")
            if connection:
                connection.rollback()
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
        Extract data from JSON file
        """
        try:
            with open(file_path, 'r') as file:
                json_data = json.load(file)
            
            return self.load_to_landing(json_data, table_name, file_name=file_path)
            
        except Exception as e:
            logger.error(f"Error extracting from file {file_path}: {str(e)}")
            return False
        
    def extract_from_string(self, json_string, table_name, source_name=None):
        """
        Extract data from JSON string (for API responses)
        """
        try:
            json_data = json.loads(json_string)
            return self.load_to_landing(json_data, table_name, file_name=source_name)
            
        except Exception as e:
            logger.error(f"Error extracting from JSON string: {str(e)}")
            return False
    
    def extract_from_object(self, json_obj, table_name, source_name=None):
        """
        Extract data from JSON object (already parsed)
        """
        try:
            return self.load_to_landing(json_obj, table_name, file_name=source_name)
            
        except Exception as e:
            logger.error(f"Error extracting from JSON object: {str(e)}")
            return False

if __name__ == "__main__":
    # Test JSON extraction
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
    
    # Test JSON extractor
    db_connector = DatabaseConnector(db_config)
    json_extractor = JSONExtractor(db_connector)
    
    logger.info("JSON Extractor initialized successfully")