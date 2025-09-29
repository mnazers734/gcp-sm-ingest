#!/usr/bin/env python3
"""
Minimal Cloud Run Job for ETL CSV Processing
"""

import os
import json
import logging
from google.cloud import storage

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class ETLProcessor:
    """Minimal ETL processor for Cloud Run Job"""
    
    def __init__(self):
        self.project_id = os.getenv('GCP_PROJECT', 'max-sm-ingest-dev')
        self.region = os.getenv('GCP_REGION', 'us-central1')
        self.bucket_name = os.getenv('STORAGE_BUCKET', 'sm-ingest-dev-8')
        self.storage_client = storage.Client(project=self.project_id)
    
    def _get_manifest_data(self):
        """Get manifest data from environment variables"""
        try:
            manifest_json = os.getenv('MANIFEST_DATA')
            if manifest_json:
                return json.loads(manifest_json)
            return None
        except Exception as e:
            logger.error(f"Error parsing manifest data: {e}")
            return None
    
    def _get_processing_plan(self):
        """Get processing plan from environment variables"""
        try:
            plan_json = os.getenv('PROCESSING_PLAN')
            if plan_json:
                return json.loads(plan_json)
            return {}
        except Exception as e:
            logger.error(f"Error parsing processing plan: {e}")
            return {}
        
    def process_manifest(self, manifest_data, processing_plan=None):
        """Process the manifest file and validate CSV files"""
        try:
            logger.info(f"Processing manifest for load_id: {manifest_data.get('load_id')}")
            
            if 'files' not in manifest_data:
                logger.error("Invalid manifest: missing 'files' key")
                return False
            
            # Use processing plan if available, otherwise process all files
            if processing_plan and 'files_to_process' in processing_plan:
                files_to_process = processing_plan['files_to_process']
                files_to_skip = processing_plan.get('files_to_skip', [])
                
                logger.info(f"Processing plan: {len(files_to_process)} files to process, {len(files_to_skip)} files to skip")
                
                # Process files in dependency order
                processing_groups = processing_plan.get('processing_groups', {})
                for group_name, group_files in processing_groups.items():
                    logger.info(f"Processing group {group_name}: {[f['name'] for f in group_files]}")
                    for file_info in group_files:
                        self._process_file(file_info)
                
                # Log skipped files
                for file_info in files_to_skip:
                    logger.info(f"Skipping file: {file_info['name']} (reason: {file_info.get('status', 'unknown')})")
                    
            else:
                # Fallback: process all files from manifest
                files = manifest_data['files']
                logger.info(f"Found {len(files)} files in manifest (no processing plan)")
                
                for file_info in files:
                    self._process_file(file_info)
                    
            logger.info("Manifest processing completed successfully")
            return True
            
        except Exception as e:
            logger.error(f"Error processing manifest: {str(e)}")
            return False
    
    def _process_file(self, file_info):
        """Process a single CSV file"""
        file_name = file_info.get('name', '')
        row_count = file_info.get('rows', 0)
        
        logger.info(f"Processing file: {file_name} (rows: {row_count})")
        
        # TODO: Add actual CSV processing logic here
        # - Download file from GCS
        # - Validate file integrity
        # - Process data according to entity type
        # - Upload to staging tables
        # - Perform upserts to production database
    
    def run(self, load_id=None, partner_id=None):
        """Main entry point for the Cloud Run Job"""
        try:
            logger.info("Starting ETL Processor Job")
            
            load_id = load_id or os.getenv('LOAD_ID', 'test-load-001')
            partner_id = partner_id or os.getenv('PARTNER_ID', 'test-partner')
                
            logger.info(f"Processing load_id: {load_id}, partner: {partner_id}")
            
            # Get manifest data from environment variables (passed by dispatcher)
            manifest_data = self._get_manifest_data()
            processing_plan = self._get_processing_plan()
            
            if not manifest_data:
                logger.error("No manifest data provided")
                return 1
                
            logger.info(f"Processing manifest for load_id: {manifest_data.get('load_id')}")
            logger.info(f"Processing plan: {len(processing_plan.get('files_to_process', []))} files to process")
            
            success = self.process_manifest(manifest_data, processing_plan)
            return 0 if success else 1
                
        except Exception as e:
            logger.error(f"Fatal error in ETL processor: {str(e)}")
            return 1


def main():
    """Main entry point for the Cloud Run Job"""
    try:
        print("=== ETL PROCESSOR STARTING ===")
        print(f"Python version: {os.sys.version}")
        print(f"Working directory: {os.getcwd()}")
        print(f"Environment variables: LOAD_ID={os.getenv('LOAD_ID')}, PARTNER_ID={os.getenv('PARTNER_ID')}")
        
        # Test basic imports
        print("Testing imports...")
        from google.cloud import storage
        print("✓ google.cloud.storage imported successfully")
        
        # Test environment variables
        required_env_vars = ['LOAD_ID', 'PARTNER_ID', 'STORAGE_BUCKET']
        missing_vars = [var for var in required_env_vars if not os.getenv(var)]
        
        if missing_vars:
            print(f"❌ Missing required environment variables: {missing_vars}")
            exit(1)
            
        print("✓ Environment variables validated")
        
        # Test ETLProcessor creation
        print("Creating ETLProcessor instance...")
        processor = ETLProcessor()
        print("✓ ETLProcessor instance created successfully")
        
        # Run the processor
        print("Starting processor.run()...")
        exit_code = processor.run()
        print(f"✓ ETL Processor Job completed with exit code: {exit_code}")
        exit(exit_code)
        
    except Exception as e:
        print(f"❌ FATAL ERROR: {e}")
        print(f"Exception type: {type(e).__name__}")
        import traceback
        print(f"Traceback: {traceback.format_exc()}")
        exit(1)


if __name__ == "__main__":
    main()