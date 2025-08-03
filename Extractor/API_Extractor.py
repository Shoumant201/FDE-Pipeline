"""
API_Extractor.py
Handles extraction of data from API endpoints
"""

import requests
import logging
from JSON_Extractor import JSONExtractor

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class APIExtractor:
    def __init__(self, api_config, db_connector):
        self.api_config = api_config
        self.db_connector = db_connector
        self.json_extractor = JSONExtractor(db_connector)
        
    def make_api_request(self, endpoint):
        """
        Make HTTP request to specific endpoint with error handling
        """
        try:
            logger.info(f"Making API request to: {endpoint}")
            
            # Send GET request with timeout
            response = requests.get(endpoint, timeout=30)
            response.raise_for_status()  # Raise an exception for bad status codes
            
            # Parse JSON response
            json_data = response.json()
            logger.info(f"Successfully retrieved data from {endpoint}")
            
            return json_data
            
        except requests.exceptions.Timeout:
            logger.error(f"Timeout error for endpoint: {endpoint}")
            return None
        except requests.exceptions.RequestException as e:
            logger.error(f"HTTP error for endpoint {endpoint}: {str(e)}")
            return None
        except ValueError as e:
            logger.error(f"JSON parsing error for endpoint {endpoint}: {str(e)}")
            return None
        except Exception as e:
            logger.error(f"Unexpected error for endpoint {endpoint}: {str(e)}")
            return None
    
    def make_api_request_with_details(self, endpoint):
        """
        Make HTTP request and return both data and response details
        """
        try:
            logger.info(f"Making API request to: {endpoint}")
            
            # Send GET request with timeout
            response = requests.get(endpoint, timeout=30)
            response.raise_for_status()  # Raise an exception for bad status codes
            
            # Parse JSON response
            json_data = response.json()
            logger.info(f"Successfully retrieved data from {endpoint} (Status: {response.status_code})")
            
            return {
                'data': json_data,
                'status_code': response.status_code,
                'headers': dict(response.headers)
            }
            
        except requests.exceptions.Timeout:
            logger.error(f"Timeout error for endpoint: {endpoint}")
            return None
        except requests.exceptions.RequestException as e:
            logger.error(f"HTTP error for endpoint {endpoint}: {str(e)}")
            return None
        except ValueError as e:
            logger.error(f"JSON parsing error for endpoint {endpoint}: {str(e)}")
            return None
        except Exception as e:
            logger.error(f"Unexpected error for endpoint {endpoint}: {str(e)}")
            return None
        
    def extract_endpoint(self, endpoint, table_name):
        """
        Extract data from a single API endpoint
        """
        try:
            # Make API request and get response details
            response_data = self.make_api_request_with_details(endpoint)
            
            if response_data is None:
                logger.error(f"Failed to retrieve data from {endpoint}")
                return False
            
            json_data = response_data['data']
            status_code = response_data['status_code']
            
            # Send data to JSONExtractor with metadata
            success = self.json_extractor.load_to_landing(
                json_data, 
                table_name, 
                file_name=endpoint,
                api_endpoint=endpoint,
                response_status=status_code
            )
            
            if success:
                logger.info(f"Successfully extracted data from {endpoint} to {table_name}")
            else:
                logger.error(f"Failed to load data from {endpoint} to {table_name}")
                
            return success
            
        except Exception as e:
            logger.error(f"Error extracting from endpoint {endpoint}: {str(e)}")
            return False
        
    def extract_all(self):
        """
        Extract data from all configured API endpoints
        """
        try:
            endpoints_config = self.api_config.get('endpoints', {})
            total_endpoints = len(endpoints_config)
            successful_extractions = 0
            
            logger.info(f"Starting extraction from {total_endpoints} API endpoints")
            
            # Loop through all endpoint mappings and process them
            for endpoint, table_name in endpoints_config.items():
                logger.info(f"Processing {endpoint} -> {table_name}")
                
                if self.extract_endpoint(endpoint, table_name):
                    successful_extractions += 1
                else:
                    logger.error(f"Failed to process {endpoint}")
            
            logger.info(f"API extraction completed: {successful_extractions}/{total_endpoints} endpoints processed successfully")
            return successful_extractions == total_endpoints
            
        except Exception as e:
            logger.error(f"Error in extract_all: {str(e)}")
            return False

if __name__ == "__main__":
    # Test API extraction
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
    
    # Test API extractor
    db_connector = DatabaseConnector(db_config)
    api_extractor = APIExtractor(config['api'], db_connector)
    
    logger.info("API Extractor initialized successfully")
    
    # Test single endpoint extraction (uncomment to test)
    # api_extractor.extract_endpoint('https://dummyjson.com/products', 'lnd_products_api')