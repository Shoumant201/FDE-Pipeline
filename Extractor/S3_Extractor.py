"""
S3_Extractor.py
Handles extraction of data from S3 bucket sources (Public S3 buckets)
"""

import requests
import pandas as pd
import json
import logging
from string import Template
from JSON_Extractor import JSONExtractor
from CSV_Extractor import CSVExtractor

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class PublicS3Extractor:
    def __init__(self, s3_config, db_connector):
        self.s3_config = s3_config
        self.db_connector = db_connector
        self.json_extractor = JSONExtractor(db_connector)
        self.csv_extractor = CSVExtractor(db_connector)
        
    def get_public_url(self, file_key):
        """
        Build the public S3 URL using bucket name, region, and file key
        """
        try:
            # Template for S3 public URL
            url_template = Template("https://$bucket_name.s3.$region.amazonaws.com/$file_key")
            
            # Substitute template variables
            public_url = url_template.substitute(
                bucket_name=self.s3_config['bucket_name'],
                region=self.s3_config['region'],
                file_key=file_key
            )
            
            logger.info(f"Generated public URL: {public_url}")
            return public_url
            
        except Exception as e:
            logger.error(f"Error generating public URL for {file_key}: {str(e)}")
            return None
        
    def extract_file(self, file_key, table_name):
        """
        Download and extract a single file from public S3 bucket
        """
        try:
            # Get the public URL
            public_url = self.get_public_url(file_key)
            if not public_url:
                return False
            
            # Download the file
            response = requests.get(public_url)
            response.raise_for_status()  # Raise an exception for bad status codes
            
            # Determine file format and process accordingly
            if file_key.lower().endswith('.json'):
                # Handle JSON file
                json_data = response.json()
                success = self.json_extractor.extract_from_object(json_data, table_name, source_name=file_key)
                
            elif file_key.lower().endswith('.csv'):
                # Handle CSV file
                from io import StringIO
                csv_content = StringIO(response.text)
                df = pd.read_csv(csv_content)
                success = self.csv_extractor.extract_from_dataframe(df, table_name, source_file=file_key)
                
            else:
                logger.error(f"Unsupported file format for {file_key}")
                return False
            
            if success:
                logger.info(f"Successfully extracted {file_key} to {table_name}")
            else:
                logger.error(f"Failed to extract {file_key} to {table_name}")
                
            return success
            
        except requests.exceptions.RequestException as e:
            logger.error(f"HTTP error downloading {file_key}: {str(e)}")
            return False
        except Exception as e:
            logger.error(f"Error extracting file {file_key}: {str(e)}")
            return False
        
    def extract_all(self):
        """
        Extract all files from S3 bucket based on config mappings
        """
        try:
            files_config = self.s3_config.get('files', {})
            total_files = len(files_config)
            successful_extractions = 0
            
            logger.info(f"Starting extraction of {total_files} files from S3")
            
            # Loop through all file mappings and process them
            for file_key, table_name in files_config.items():
                logger.info(f"Processing {file_key} -> {table_name}")
                
                if self.extract_file(file_key, table_name):
                    successful_extractions += 1
                else:
                    logger.error(f"Failed to process {file_key}")
            
            logger.info(f"S3 extraction completed: {successful_extractions}/{total_files} files processed successfully")
            return successful_extractions == total_files
            
        except Exception as e:
            logger.error(f"Error in extract_all: {str(e)}")
            return False

if __name__ == "__main__":
    # Test S3 extraction
    from Database_Connector import DatabaseConnector
    import yaml
    import os
    from dotenv import load_dotenv
    
    # Load environment variables and config
    load_dotenv()
    
    with open('config.yaml', 'r') as file:
        config = yaml.safe_load(file)
    
    # Substitute environment variables in S3 config
    s3_config = {
        'bucket_name': os.getenv('S3_BUCKET_NAME'),
        'region': os.getenv('AWS_REGION'),
        'files': config['s3']['files']
    }
    
    db_config = {
        'host': os.getenv('DB_HOST'),
        'database': config['database']['database'],
        'user': os.getenv('DB_USER'),
        'password': os.getenv('DB_PASSWORD'),
        'port': os.getenv('DB_PORT')
    }
    
    # Test S3 extractor
    db_connector = DatabaseConnector(db_config)
    s3_extractor = PublicS3Extractor(s3_config, db_connector)
    
    logger.info("Public S3 Extractor initialized successfully")
    
    # Test single file extraction (uncomment to test)
    # s3_extractor.extract_file('JSON/products.json', 'lnd_products_json')