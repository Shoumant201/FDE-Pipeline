"""
Main_Extractor.py
Main orchestrator script that coordinates all extraction processes
"""

import logging
import yaml
import os
from dotenv import load_dotenv
from string import Template
from S3_Extractor import PublicS3Extractor
from API_Extractor import APIExtractor
from Database_Connector import DatabaseConnector

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class MainExtractor:
    def __init__(self, config_path=None):
        if config_path is None:
            # Get the directory where this script is located
            script_dir = os.path.dirname(os.path.abspath(__file__))
            
            # Try to find config.yaml in multiple locations
            possible_paths = [
                'config.yaml',  # Current working directory
                '../config.yaml',  # Parent directory
                os.path.join(script_dir, 'config.yaml'),  # Same directory as script
                os.path.join(script_dir, '..', 'config.yaml')  # Parent of script directory
            ]
            
            config_path = None
            for path in possible_paths:
                if os.path.exists(path):
                    config_path = path
                    logger.info(f"Found config.yaml at: {path}")
                    break
            
            if config_path is None:
                raise FileNotFoundError("config.yaml not found in any expected location")
        
        self.config = self.load_config(config_path)
        self.db_connector = DatabaseConnector(self.config['database'])
        self.s3_extractor = PublicS3Extractor(self.config['s3'], self.db_connector)
        self.api_extractor = APIExtractor(self.config['api'], self.db_connector)
        
    def load_config(self, config_path):
        """
        Load configuration with environment variable substitution
        """
        try:
            # Get the directory where this script is located
            script_dir = os.path.dirname(os.path.abspath(__file__))
            
            # Try to find .env file in multiple locations
            possible_env_paths = [
                '.env',  # Current working directory
                '../.env',  # Parent directory
                os.path.join(script_dir, '.env'),  # Same directory as script
                os.path.join(script_dir, '..', '.env')  # Parent of script directory
            ]
            
            env_loaded = False
            for env_path in possible_env_paths:
                if os.path.exists(env_path):
                    load_dotenv(env_path)
                    logger.info(f"Loaded environment variables from: {env_path}")
                    env_loaded = True
                    break
            
            if not env_loaded:
                load_dotenv()  # Try default locations
            
            # Load config file
            with open(config_path, 'r') as file:
                config = yaml.safe_load(file)
            
            # Substitute environment variables in database config
            db_config = {
                'host': os.getenv('DB_HOST'),
                'database': config['database']['database'],
                'user': os.getenv('DB_USER'),
                'password': os.getenv('DB_PASSWORD'),
                'port': os.getenv('DB_PORT')
            }
            
            # Substitute environment variables in S3 config
            s3_config = {
                'bucket_name': os.getenv('S3_BUCKET_NAME'),
                'region': os.getenv('AWS_REGION'),
                'files': config['s3']['files']
            }
            
            # API config doesn't need environment substitution
            api_config = config['api']
            
            return {
                'database': db_config,
                's3': s3_config,
                'api': api_config
            }
            
        except Exception as e:
            logger.error(f"Error loading config: {str(e)}")
            raise
    
    def get_table_columns(self, table_name):
        """
        Utility method to get table columns from database
        """
        try:
            connection = self.db_connector.get_connection()
            cursor = connection.cursor()
            
            query = """
            SELECT column_name 
            FROM information_schema.columns 
            WHERE table_name = %s
            ORDER BY ordinal_position
            """
            
            cursor.execute(query, (table_name,))
            columns = [row[0] for row in cursor.fetchall()]
            cursor.close()
            
            return columns
            
        except Exception as e:
            logger.error(f"Error getting table columns for {table_name}: {str(e)}")
            return []
    
    def truncate_table(self, table_name):
        """
        Utility method to truncate a specific table
        """
        try:
            success = self.db_connector.truncate_table(table_name)
            if success:
                logger.info(f"Successfully truncated table {table_name}")
            else:
                logger.error(f"Failed to truncate table {table_name}")
            return success
            
        except Exception as e:
            logger.error(f"Error truncating table {table_name}: {str(e)}")
            return False
    
    def extract_from_s3(self):
        """
        Extract data from S3 sources only
        """
        try:
            logger.info("Starting S3 extraction process...")
            success = self.s3_extractor.extract_all()
            
            if success:
                logger.info("S3 extraction completed successfully")
            else:
                logger.error("S3 extraction completed with errors")
                
            return success
            
        except Exception as e:
            logger.error(f"Error in S3 extraction: {str(e)}")
            return False
    
    def extract_from_api(self):
        """
        Extract data from API sources only
        """
        try:
            logger.info("Starting API extraction process...")
            success = self.api_extractor.extract_all()
            
            if success:
                logger.info("API extraction completed successfully")
            else:
                logger.error("API extraction completed with errors")
                
            return success
            
        except Exception as e:
            logger.error(f"Error in API extraction: {str(e)}")
            return False
    
    def extract_all(self):
        """
        Extract data from both S3 and API sources
        """
        try:
            logger.info("Starting full extraction process (S3 + API)...")
            
            # Extract from S3
            s3_success = self.extract_from_s3()
            
            # Extract from API
            api_success = self.extract_from_api()
            
            # Overall success if both succeed
            overall_success = s3_success and api_success
            
            if overall_success:
                logger.info("Full extraction process completed successfully")
            else:
                logger.warning("Full extraction process completed with some errors")
                
            return overall_success
            
        except Exception as e:
            logger.error(f"Error in full extraction: {str(e)}")
            return False
        finally:
            # Always close database connections
            self.db_connector.close_connection()
    
    def run_extraction(self, source='all'):
        """
        Main method to orchestrate extraction processes
        
        Args:
            source (str): 'all', 's3', or 'api' to specify which sources to extract from
        """
        try:
            logger.info(f"Starting extraction process for source: {source}")
            
            if source.lower() == 's3':
                return self.extract_from_s3()
            elif source.lower() == 'api':
                return self.extract_from_api()
            elif source.lower() == 'all':
                return self.extract_all()
            else:
                logger.error(f"Invalid source specified: {source}. Use 'all', 's3', or 'api'")
                return False
                
        except Exception as e:
            logger.error(f"Error in run_extraction: {str(e)}")
            return False

if __name__ == "__main__":
    import sys
    
    # Parse command line arguments
    source = 'all'  # default
    if len(sys.argv) > 1:
        source = sys.argv[1]
    
    try:
        # Initialize and run main extractor
        main_extractor = MainExtractor()
        
        logger.info("Main Extractor initialized successfully")
        logger.info(f"Starting extraction for source: {source}")
        
        # Run extraction
        success = main_extractor.run_extraction(source)
        
        if success:
            logger.info("Extraction process completed successfully!")
            sys.exit(0)
        else:
            logger.error("Extraction process failed!")
            sys.exit(1)
            
    except Exception as e:
        logger.error(f"Fatal error in main execution: {str(e)}")
        sys.exit(1)