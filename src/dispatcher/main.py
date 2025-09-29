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
            if len(path_parts) < 3:
                logger.error(f"Invalid path structure: {object_name}")
                return {'status': 'error', 'reason': 'invalid_path_structure'}
            
            partner_id = path_parts[1]  # imports/{partner_id}/...
            load_id = path_parts[2]     # imports/{partner_id}/{load_id}/...
            
            # Download and validate manifest
            manifest_data = self._download_and_validate_manifest(bucket_name, object_name)
            if not manifest_data:
                return {'status': 'error', 'reason': 'invalid_manifest'}
            
            # Validate required files are present
            file_validation = self._validate_required_files(bucket_name, path_parts)
            if not file_validation['valid']:
                return {'status': 'error', 'reason': 'missing_required_files', 'details': file_validation}
            
            # Launch ETL job
            job_result = self._launch_etl_job(partner_id, load_id, manifest_data)
            
            # Extract processing plan for response
            processing_plan = manifest_data.get('processing_plan', {})
            
            return {
                'status': 'success',
                'load_id': load_id,
                'partner_id': partner_id,
                'job_name': job_result.get('name'),
                'processing_summary': {
                    'files_to_process': processing_plan.get('files_to_process', 0),
                    'files_to_skip': processing_plan.get('files_to_skip', 0),
                    'total_rows': processing_plan.get('total_rows', 0),
                    'processing_groups': len(processing_plan.get('processing_groups', {}))
                },
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
            
            # Process manifest to determine which files need processing
            processing_plan = self._create_processing_plan(manifest_data)
            manifest_data['processing_plan'] = processing_plan
            
            logger.info(f"Manifest validated: {len(files)} files, processing plan created")
            return manifest_data
            
        except Exception as e:
            logger.error(f"Failed to download/validate manifest: {e}")
            return None
    
    def _create_processing_plan(self, manifest_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Create a processing plan based on manifest data.
        
        Determines which CSV files need processing based on row counts and
        creates a dependency-ordered processing plan.
        
        Args:
            manifest_data: Parsed manifest.json data
            
        Returns:
            Dictionary containing processing plan with file information and order
        """
        files = manifest_data.get('files', [])
        
        # Define file processing order and dependencies
        # Customer is always required (never empty)
        # Vehicle depends on Customer
        # Invoice depends on Vehicle  
        # Line Item depends on Invoice
        # Payments depends on Invoice
        # Inventory Parts and Suppliers are independent
        
        processing_order = [
            'customers.csv',      # Always required, never empty
            'vehicles.csv',       # Depends on customers
            'invoices.csv',       # Depends on vehicles  
            'line_items.csv',     # Depends on invoices
            'payments.csv',       # Depends on invoices
            'inventory_parts.csv', # Independent
            'suppliers.csv'       # Independent
        ]
        
        # Create file lookup by name
        file_lookup = {file_info['name']: file_info for file_info in files}
        
        # Determine which files need processing (non-zero row count)
        files_to_process = []
        files_to_skip = []
        
        for filename in processing_order:
            if filename in file_lookup:
                file_info = file_lookup[filename]
                row_count = file_info.get('rows', 0)
                
                if row_count > 0:
                    files_to_process.append({
                        'name': filename,
                        'rows': row_count,
                        'sha256': file_info.get('sha256', ''),
                        'status': 'to_process'
                    })
                    logger.info(f"File {filename}: {row_count} rows - WILL PROCESS")
                else:
                    files_to_skip.append({
                        'name': filename,
                        'rows': 0,
                        'sha256': file_info.get('sha256', ''),
                        'status': 'skip_empty'
                    })
                    logger.info(f"File {filename}: 0 rows - SKIP (empty file)")
            else:
                logger.warning(f"Required file {filename} not found in manifest")
                files_to_skip.append({
                    'name': filename,
                    'rows': 0,
                    'sha256': '',
                    'status': 'missing'
                })
        
        # Create dependency groups for processing
        dependency_groups = {
            'group_1': ['customers.csv'],  # Always first
            'group_2': ['vehicles.csv'],    # Depends on customers
            'group_3': ['invoices.csv'],   # Depends on vehicles
            'group_4': ['line_items.csv', 'payments.csv'],  # Both depend on invoices
            'group_5': ['inventory_parts.csv', 'suppliers.csv']  # Independent
        }
        
        # Organize files by dependency groups
        processing_groups = {}
        for group_name, filenames in dependency_groups.items():
            group_files = []
            for filename in filenames:
                file_info = next((f for f in files_to_process if f['name'] == filename), None)
                if file_info:
                    group_files.append(file_info)
            if group_files:
                processing_groups[group_name] = group_files
        
        # Calculate total rows for processing
        total_rows = sum(file_info['rows'] for file_info in files_to_process)
        
        processing_plan = {
            'load_id': manifest_data.get('load_id'),
            'total_files': len(files),
            'files_to_process': len(files_to_process),
            'files_to_skip': len(files_to_skip),
            'total_rows': total_rows,
            'processing_groups': processing_groups,
            'files_to_process': files_to_process,
            'files_to_skip': files_to_skip,
            'processing_order': processing_order
        }
        
        logger.info(f"Processing plan created: {len(files_to_process)} files to process, "
                   f"{len(files_to_skip)} files to skip, {total_rows} total rows")
        
        return processing_plan
    
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
    
    def _launch_etl_job(self, partner_id: str, load_id: str, manifest_data: Dict[str, Any]) -> Dict[str, Any]:
        """Launch Cloud Run Job for ETL processing."""
        try:
            # Prepare job execution request
            job_path = f"projects/{self.project_id}/locations/{self.region}/jobs/{self.job_name}"
            
            # Extract processing plan from manifest data
            processing_plan = manifest_data.get('processing_plan', {})
            
            # Set environment variables for the job
            env_vars = {
                'LOAD_ID': load_id,
                'PARTNER_ID': partner_id,
                'MANIFEST_DATA': json.dumps(manifest_data),
                'PROCESSING_PLAN': json.dumps(processing_plan),
                'FILES_TO_PROCESS': json.dumps(processing_plan.get('files_to_process', [])),
                'FILES_TO_SKIP': json.dumps(processing_plan.get('files_to_skip', [])),
                'PROCESSING_GROUPS': json.dumps(processing_plan.get('processing_groups', {})),
                'TOTAL_ROWS': str(processing_plan.get('total_rows', 0)),
                'FILES_COUNT': str(processing_plan.get('files_to_process', 0))
            }
            
            # Log processing plan details
            logger.info(f"Launching ETL job with processing plan:")
            logger.info(f"  - Files to process: {processing_plan.get('files_to_process', 0)}")
            logger.info(f"  - Files to skip: {processing_plan.get('files_to_skip', 0)}")
            logger.info(f"  - Total rows: {processing_plan.get('total_rows', 0)}")
            logger.info(f"  - Processing groups: {len(processing_plan.get('processing_groups', {}))}")
            
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
            logger.info(f"ETL job launched successfully")
            
            return {
                'job_path': job_path,
                'status': 'launched',
                'load_id': load_id,
                'processing_plan': processing_plan
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


@app.route('/trigger/<partner_id>/<load_id>', methods=['POST'])
def manual_trigger(partner_id: str, load_id: str):
    """
    Manual trigger endpoint for testing.
    
    Args:
        partner_id: Partner identifier
        load_id: Load identifier
    """
    try:
        # Construct manifest path
        manifest_path = f"imports/{partner_id}/{load_id}/manifest.json"
        
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


@app.route('/processing-plan/<partner_id>/<load_id>', methods=['GET'])
def get_processing_plan(partner_id: str, load_id: str):
    """
    Get detailed processing plan for a specific load.
    
    This endpoint allows inspection of what files will be processed
    and in what order based on the manifest.
    """
    try:
        # Construct manifest path
        manifest_path = f"imports/{partner_id}/{load_id}/manifest.json"
        
        # Download and process manifest
        manifest_data = get_dispatcher()._download_and_validate_manifest(GCS_BUCKET, manifest_path)
        
        if not manifest_data:
            return jsonify({'status': 'error', 'reason': 'manifest_not_found'}), 404
        
        processing_plan = manifest_data.get('processing_plan', {})
        
        return jsonify({
            'load_id': load_id,
            'partner_id': partner_id,
            'processing_plan': processing_plan,
            'timestamp': datetime.now().isoformat()
        })
        
    except Exception as e:
        logger.error(f"Error getting processing plan: {e}")
        return jsonify({'status': 'error', 'reason': str(e)}), 500


if __name__ == '__main__':
    port = int(os.getenv('PORT', 8080))
    app.run(host='0.0.0.0', port=port, debug=False)
