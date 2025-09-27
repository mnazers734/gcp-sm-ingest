"""
Cloud Run Dispatcher Service for ETL Pipeline.

This service handles GCS event triggers and dispatches ETL processing jobs.
Implements the dispatcher component from the PRD architecture.
"""

import os
import json
import logging
from datetime import datetime
from typing import Dict, Any, Optional
from flask import Flask, request, jsonify
from google.cloud import storage
from google.cloud import run_v2
from cloudevents.http import from_http

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = Flask(__name__)

# Load configuration from environment (matches deploy-dispatcher.sh)
PROJECT_ID = os.getenv('GCP_PROJECT')
REGION = os.getenv('GCP_REGION', 'us-central1')
JOB_NAME = os.getenv('JOB_NAME', 'etl-processor')
GCS_BUCKET = os.getenv('STORAGE_BUCKET')
#SERVICE_URL = os.getenv('SERVICE_URL')

# Validate required environment variables
if not PROJECT_ID:
    logger.error("GCP_PROJECT environment variable is required")
if not GCS_BUCKET:
    logger.error("STORAGE_BUCKET environment variable is required")

# Initialize clients
storage_client = storage.Client()
run_client = run_v2.JobsClient()

class ETLDispatcher:
    """
    Dispatcher service that handles GCS events and launches ETL jobs.
    
    Implements the dispatcher component from the PRD:
    - Receives GCS event when manifest.json is uploaded
    - Validates file structure and manifest
    - Launches Cloud Run Job for ETL processing
    """
    
    def __init__(self):
        self.project_id = PROJECT_ID
        self.region = REGION
        self.job_name = JOB_NAME
        self.gcs_bucket = GCS_BUCKET
        
    def process_gcs_event(self, event_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Process GCS event and launch ETL job.
        
        Args:
            event_data: GCS event data from Eventarc
            
        Returns:
            Dictionary with processing results
        """
        try:
            # Extract bucket and object information
            bucket_name = event_data.get('bucket')
            object_name = event_data.get('name')
            
            logger.info(f"Processing GCS event: {bucket_name}/{object_name}")
            
            # Validate this is a manifest.json file
            if not object_name.endswith('manifest.json'):
                logger.warning(f"Ignoring non-manifest file: {object_name}")
                return {'status': 'ignored', 'reason': 'not_manifest_file'}
            
            # Extract load information from path
            path_parts = object_name.split('/')
            if len(path_parts) < 4:
                logger.error(f"Invalid path structure: {object_name}")
                return {'status': 'error', 'reason': 'invalid_path_structure'}
            
            partner_id = path_parts[1]  # imports/{partner_id}/...
            shop_id = path_parts[2]     # imports/{partner_id}/{shop_id}/...
            load_id = path_parts[3]     # imports/{partner_id}/{shop_id}/{load_id}/...
            
            # Download and validate manifest
            manifest_data = self._download_and_validate_manifest(bucket_name, object_name)
            if not manifest_data:
                return {'status': 'error', 'reason': 'invalid_manifest'}
            
            # Validate required files are present
            file_validation = self._validate_required_files(bucket_name, path_parts)
            if not file_validation['valid']:
                return {'status': 'error', 'reason': 'missing_required_files', 'details': file_validation}
            
            # Launch ETL job
            job_result = self._launch_etl_job(partner_id, shop_id, load_id, manifest_data)
            
            return {
                'status': 'success',
                'load_id': load_id,
                'partner_id': partner_id,
                'shop_id': shop_id,
                'job_name': job_result.get('name'),
                'timestamp': datetime.now().isoformat()
            }
            
        except Exception as e:
            logger.error(f"Error processing GCS event: {e}")
            return {'status': 'error', 'reason': str(e)}
    
    def _download_and_validate_manifest(self, bucket_name: str, object_name: str) -> Optional[Dict[str, Any]]:
        """Download and validate manifest.json file."""
        try:
            bucket = storage_client.bucket(bucket_name)
            blob = bucket.blob(object_name)
            
            # Download manifest content
            manifest_content = blob.download_as_text()
            manifest_data = json.loads(manifest_content)
            
            # Validate manifest structure
            required_fields = ['load_id', 'files']
            if not all(field in manifest_data for field in required_fields):
                logger.error("Manifest missing required fields")
                return None
            
            # Validate files array
            files = manifest_data.get('files', [])
            if not isinstance(files, list):
                logger.error("Manifest files must be an array")
                return None
            
            # Validate each file entry
            required_file_fields = ['name', 'rows', 'sha256']
            for file_info in files:
                if not all(field in file_info for field in required_file_fields):
                    logger.error(f"File entry missing required fields: {file_info}")
                    return None
            
            logger.info(f"Manifest validated: {len(files)} files")
            return manifest_data
            
        except Exception as e:
            logger.error(f"Failed to download/validate manifest: {e}")
            return None
    
    def _validate_required_files(self, bucket_name: str, path_parts: list) -> Dict[str, Any]:
        """Validate that all required CSV files are present."""
        required_files = [
            'customers.csv',
            'vehicles.csv', 
            'invoices.csv',
            'line_items.csv',
            'payments.csv',
            'inventory_parts.csv',
            'suppliers.csv'
        ]
        
        missing_files = []
        present_files = []
        
        # Check each required file
        for filename in required_files:
            file_path = '/'.join(path_parts[:-1]) + '/' + filename
            try:
                bucket = storage_client.bucket(bucket_name)
                blob = bucket.blob(file_path)
                if blob.exists():
                    present_files.append(filename)
                else:
                    missing_files.append(filename)
            except Exception as e:
                logger.warning(f"Error checking file {filename}: {e}")
                missing_files.append(filename)
        
        return {
            'valid': len(missing_files) == 0,
            'present_files': present_files,
            'missing_files': missing_files,
            'total_required': len(required_files),
            'total_present': len(present_files)
        }
    
    def _launch_etl_job(self, partner_id: str, shop_id: str, load_id: str, manifest_data: Dict[str, Any]) -> Dict[str, Any]:
        """Launch Cloud Run Job for ETL processing."""
        try:
            # Prepare job execution request
            job_path = f"projects/{self.project_id}/locations/{self.region}/jobs/{self.job_name}"
            
            # Set environment variables for the job
            env_vars = {
                'LOAD_ID': load_id,
                'PARTNER_ID': partner_id,
                'SHOP_ID': shop_id,
                'MANIFEST_DATA': json.dumps(manifest_data)
            }
            
            # Create execution request
            execution_request = run_v2.RunJobRequest(
                name=job_path,
                overrides=run_v2.RunJobRequest.Overrides(
                    container_overrides=[
                        run_v2.RunJobRequest.Overrides.ContainerOverride(
                            name='etl-processor',
                            env=[
                                run_v2.EnvVar(name=key, value=value)
                                for key, value in env_vars.items()
                            ]
                        )
                    ]
                )
            )
            
            # Execute the job
            operation = run_client.run_job(request=execution_request)
            logger.info(f"ETL job launched: {operation.name}")
            
            return {
                'name': operation.name,
                'status': 'launched',
                'load_id': load_id
            }
            
        except Exception as e:
            logger.error(f"Failed to launch ETL job: {e}")
            raise


# Initialize dispatcher (only if required env vars are present)
dispatcher = None

def get_dispatcher():
    global dispatcher
    if dispatcher is None:
        if not PROJECT_ID or not GCS_BUCKET:
            raise ValueError("Required environment variables not set")
        dispatcher = ETLDispatcher()
    return dispatcher


@app.route('/health', methods=['GET'])
def health_check():
    """Health check endpoint."""
    return jsonify({'status': 'healthy', 'timestamp': datetime.now().isoformat()})


@app.route('/ready', methods=['GET'])
def readiness_check():
    """Readiness check endpoint."""
    return jsonify({'status': 'ready', 'timestamp': datetime.now().isoformat()})


@app.route('/', methods=['POST'])
def handle_gcs_event():
    """
    Handle GCS event from Eventarc.
    
    This endpoint receives CloudEvents from GCS when files are uploaded.
    """
    try:
        # Parse CloudEvent
        event = from_http(request.headers, request.get_data())
        
        # Extract GCS event data
        event_data = event.data if hasattr(event, 'data') else {}
        
        # Process the event
        result = get_dispatcher().process_gcs_event(event_data)
        
        logger.info(f"Event processing result: {result}")
        return jsonify(result)
        
    except Exception as e:
        logger.error(f"Error handling GCS event: {e}")
        return jsonify({'status': 'error', 'reason': str(e)}), 500


@app.route('/trigger/<partner_id>/<shop_id>/<load_id>', methods=['POST'])
def manual_trigger(partner_id: str, shop_id: str, load_id: str):
    """
    Manual trigger endpoint for testing.
    
    Args:
        partner_id: Partner identifier
        shop_id: Shop identifier  
        load_id: Load identifier
    """
    try:
        # Construct manifest path
        manifest_path = f"imports/{partner_id}/{shop_id}/{load_id}/manifest.json"
        
        # Create mock event data
        event_data = {
            'bucket': GCS_BUCKET,
            'name': manifest_path
        }
        
        # Process the event
        result = get_dispatcher().process_gcs_event(event_data)
        
        return jsonify(result)
        
    except Exception as e:
        logger.error(f"Error in manual trigger: {e}")
        return jsonify({'status': 'error', 'reason': str(e)}), 500


@app.route('/status/<load_id>', methods=['GET'])
def get_load_status(load_id: str):
    """Get status of a specific load."""
    try:
        # This would typically query a database or job status
        # For now, return a placeholder
        return jsonify({
            'load_id': load_id,
            'status': 'processing',
            'timestamp': datetime.now().isoformat()
        })
        
    except Exception as e:
        logger.error(f"Error getting load status: {e}")
        return jsonify({'status': 'error', 'reason': str(e)}), 500


if __name__ == '__main__':
    port = int(os.getenv('PORT', 8080))
    app.run(host='0.0.0.0', port=port, debug=False)
