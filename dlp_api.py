"""
DLP Test Automation - API Edition
Direct Salesforce REST API calls - No Playwright, No Browser!

Run with: python dlp_api_app.py
"""

from flask import Flask, render_template, request, jsonify
import os
import json
import requests
import jwt
import time
import threading
import uuid
import openpyxl
from io import BytesIO
from datetime import datetime
from functools import wraps
from dotenv import load_dotenv

load_dotenv()

app = Flask(__name__)

# ============================================================
# CONFIGURATION
# ============================================================

# Salesforce Object API Names
LOADED_FILE_OBJECT = "Transaction_Log__c"
BATCH_OBJECT = "Detail_Transaction_log__c"

# Field mappings
LOADED_FILE_FIELDS = {
    'name': 'Name',
    'interface': 'Interface_Name__c',
    'status': 'Status__c'
}

BATCH_FIELDS = {
    'parent_lookup': 'Transaction_Log__c',
    'request_json': 'Request_JSON__c',
    'status': 'Status__c',
    'error': 'Error__c'
}

# Status values
SUCCESS_STATUSES = ['Heroku Completed', '♓️ Heroku Completed', 'Completed']
FAILURE_STATUSES = ['Salesforce Failed', 'Heroku Failed', '🔴 Salesforce Failed', '🔴 Heroku Failed', 'Failed']

# Session storage
test_sessions = {}

# ============================================================
# AUTHENTICATION
# ============================================================

def check_auth(username, password):
    """Check basic auth credentials"""
    return (username == os.getenv('APP_USER') and 
            password == os.getenv('APP_PASS'))


def authenticate():
    """Send 401 response for basic auth"""
    return jsonify({'message': 'Authentication required'}), 401, {
        'WWW-Authenticate': 'Basic realm="Login Required"'
    }


def requires_auth(f):
    """Decorator for basic authentication"""
    @wraps(f)
    def decorated(*args, **kwargs):
        auth = request.authorization
        if not auth or not check_auth(auth.username, auth.password):
            return authenticate()
        return f(*args, **kwargs)
    return decorated


# ============================================================
# SALESFORCE API HELPER CLASS
# ============================================================

class SalesforceAPI:
    """Direct Salesforce REST API client"""
    
    def __init__(self, config: dict):
        self.client_id = config['client_id']
        self.username = config['username']
        self.private_key_path = config.get('private_key_path', 'private.key')
        self.login_url = config.get('login_url', 'https://test.salesforce.com')
        self.org_url = config.get('org_url', '')
        
        self.access_token = None
        self.instance_url = None
    
    def authenticate(self) -> dict:
        """Authenticate using JWT Bearer Token"""
        with open(self.private_key_path, 'r') as f:
            private_key = f.read()
        
        payload = {
            'iss': self.client_id,
            'sub': self.username,
            'aud': self.login_url,
            'exp': int(time.time()) + 300
        }
        
        encoded_jwt = jwt.encode(payload, private_key, algorithm='RS256')
        
        response = requests.post(
            f"{self.login_url}/services/oauth2/token",
            data={
                'grant_type': 'urn:ietf:params:oauth:grant-type:jwt-bearer',
                'assertion': encoded_jwt
            }
        )
        
        if response.status_code != 200:
            raise Exception(f"Authentication failed: {response.text}")
        
        token_data = response.json()
        self.access_token = token_data['access_token']
        self.instance_url = token_data['instance_url']
        
        if not self.org_url:
            self.org_url = self.instance_url
        
        return {
            'access_token': self.access_token,
            'instance_url': self.instance_url
        }
    
    def _headers(self) -> dict:
        """Get headers for API requests"""
        return {
            'Authorization': f'Bearer {self.access_token}',
            'Content-Type': 'application/json'
        }
    
    def create_loaded_file(self, job_name: str, interface_name: str) -> dict:
        """Create a Transaction_Log__c record"""
        record_data = {
            LOADED_FILE_FIELDS['name']: job_name,
            LOADED_FILE_FIELDS['interface']: interface_name
        }
        
        url = f"{self.instance_url}/services/data/v59.0/sobjects/{LOADED_FILE_OBJECT}"
        response = requests.post(url, headers=self._headers(), json=record_data)
        
        if response.status_code not in [200, 201]:
            raise Exception(f"Create Loaded File failed: {response.text}")
        
        record_id = response.json()['id']
        view_url = f"{self.org_url}/lightning/r/{LOADED_FILE_OBJECT}/{record_id}/view"
        
        return {
            'id': record_id,
            'name': job_name,
            'view_url': view_url
        }
    
    def create_batch_record(self, loaded_file_id: str, request_json: dict, batch_number: int = 1) -> dict:
        """Create a Detail_Transaction_log__c record"""
        json_string = json.dumps(request_json, indent=2) if isinstance(request_json, dict) else request_json
        
        record_data = {
            BATCH_FIELDS['parent_lookup']: loaded_file_id,
            BATCH_FIELDS['request_json']: json_string,
            BATCH_FIELDS['status']: 'Queued',
            BATCH_FIELDS['error']: ''
        }
        
        url = f"{self.instance_url}/services/data/v59.0/sobjects/{BATCH_OBJECT}"
        response = requests.post(url, headers=self._headers(), json=record_data)
        
        if response.status_code not in [200, 201]:
            raise Exception(f"Create Batch failed: {response.text}")
        
        record_id = response.json()['id']
        view_url = f"{self.org_url}/lightning/r/{BATCH_OBJECT}/{record_id}/view"
        
        return {
            'id': record_id,
            'batch_number': batch_number,
            'view_url': view_url
        }
    
    def launch_job(self, loaded_file_id: str) -> dict:
        """Launch the job using Anonymous Apex execution"""
        apex_code = f"ErrorRecoveryController.launchTalendJob('{loaded_file_id}');"
        
        url = f"{self.instance_url}/services/data/v59.0/tooling/executeAnonymous"
        response = requests.get(
            url,
            headers=self._headers(),
            params={'anonymousBody': apex_code}
        )
        
        if response.status_code == 200:
            result = response.json()
            if result.get('success'):
                return {'success': True, 'method': 'anonymous_apex'}
            else:
                compile_problem = result.get('compileProblem', '')
                exception_message = result.get('exceptionMessage', '')
                raise Exception(f"Apex execution failed: {compile_problem} {exception_message}")
        
        raise Exception(f"Launch failed: {response.text}")
    
    def get_status(self, loaded_file_id: str) -> dict:
        """Get status of a Loaded File record"""
        query = f"""
            SELECT Id, Name, Status__c, Interface_Name__c, 
                   Processed__c, Failed__c, TotalSucceeded__c
            FROM {LOADED_FILE_OBJECT} 
            WHERE Id = '{loaded_file_id}'
        """
        
        url = f"{self.instance_url}/services/data/v59.0/query"
        response = requests.get(url, headers=self._headers(), params={'q': query})
        
        if response.status_code != 200:
            raise Exception(f"Query failed: {response.text}")
        
        records = response.json()['records']
        if not records:
            raise Exception(f"Record not found: {loaded_file_id}")
        
        return records[0]
    
    def wait_for_completion(self, loaded_file_id: str, timeout: int = 300, poll_interval: int = 10, log_callback=None) -> dict:
        """Wait for job completion with polling"""
        start_time = time.time()
        
        while time.time() - start_time < timeout:
            elapsed = int(time.time() - start_time)
            
            try:
                record = self.get_status(loaded_file_id)
                status = record.get('Status__c', 'Unknown')
                
                if log_callback:
                    log_callback(f"[{elapsed}s] Status: {status}")
                
                # Check for success
                if any(s in str(status) for s in SUCCESS_STATUSES):
                    return {'success': True, 'status': status, 'duration': elapsed}
                
                # Check for failure
                if any(s in str(status) for s in FAILURE_STATUSES):
                    return {'success': False, 'status': status, 'duration': elapsed}
                
            except Exception as e:
                if log_callback:
                    log_callback(f"[{elapsed}s] Error checking status: {e}")
            
            time.sleep(poll_interval)
        
        return {'success': False, 'status': 'Timeout', 'duration': timeout}


# ============================================================
# HELPER FUNCTIONS
# ============================================================

def format_log(message: str, level: str = 'INFO') -> str:
    """Format log message with timestamp"""
    timestamp = datetime.now().strftime('%H:%M:%S')
    return f"[{timestamp}] {level}: {message}"


def process_excel_file(file_content: bytes) -> dict:
    """Extract JSON data from Excel Column A"""
    workbook = openpyxl.load_workbook(BytesIO(file_content))
    worksheet = workbook.active
    
    processed_data = {}
    valid_entries = 0
    
    for idx, cell in enumerate(worksheet['A'], 1):
        if cell.value is None or str(cell.value).strip() == '':
            continue
        
        try:
            cell_json = json.loads(str(cell.value).strip())
            key = f"record_{valid_entries + 1:03d}"
            processed_data[key] = cell_json
            valid_entries += 1
        except json.JSONDecodeError as e:
            print(f"Row {idx}: Invalid JSON - {e}")
            continue
    
    if valid_entries == 0:
        raise ValueError("No valid JSON data found in Excel Column A")
    
    return processed_data


def get_sf_config() -> dict:
    """Get Salesforce configuration from environment"""
    return {
        'client_id': os.getenv('SALESFORCE_CLIENT_ID'),
        'username': os.getenv('SALESFORCE_USERNAME'),
        'private_key_path': os.getenv('SALESFORCE_PRIVATE_KEY_PATH', 'private.key'),
        'login_url': os.getenv('SALESFORCE_LOGIN_URL', 'https://test.salesforce.com'),
        'org_url': os.getenv('SALESFORCE_ORG_URL', '')
    }


# ============================================================
# MAIN WORKFLOW
# ============================================================

def run_api_workflow(test_data: dict, session_id: str):
    """Run the complete workflow using API calls"""
    
    if session_id not in test_sessions:
        return
    
    session = test_sessions[session_id]
    session['running'] = True
    session['logs'] = []
    session['start_time'] = datetime.now()
    
    def log(message, level='INFO'):
        session['logs'].append(format_log(message, level))
        print(f"[{session_id[:8]}] {level}: {message}")
    
    try:
        log("🚀 DLP Test Automation - API Edition", "INFO")
        log("=" * 50, "INFO")
        log("No browser needed! Direct API calls.", "INFO")
        log("", "INFO")
        
        # Get config
        sf_config = get_sf_config()
        
        # Create API client
        log("🔐 Authenticating with Salesforce...", "INFO")
        sf = SalesforceAPI(sf_config)
        auth_result = sf.authenticate()
        log(f"✅ Authenticated! Instance: {auth_result['instance_url']}", "SUCCESS")
        
        # Process input data
        file_type = test_data.get('fileType', 'json')
        
        if file_type == 'excel':
            log("📊 Processing Excel file...", "INFO")
            json_data = process_excel_file(test_data['fileContent'])
            log(f"✅ Extracted {len(json_data)} records from Excel", "SUCCESS")
        else:
            json_data = test_data.get('jsonData', {})
            log(f"📋 Using JSON data ({len(json_data)} records)", "INFO")
        
        # Generate job name
        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        custom_name = test_data.get('loadedFileName', '').strip()
        job_name = f"{custom_name} : {timestamp}" if custom_name else f"API Test : {timestamp}"
        
        interface_name = test_data.get('interfaceName')
        
        # Step 1: Create Loaded File
        log("", "INFO")
        log("=" * 50, "INFO")
        log("📁 STEP 1: Creating Loaded File", "INFO")
        log(f"   Job Name: {job_name}", "INFO")
        log(f"   Interface: {interface_name}", "INFO")
        
        loaded_file = sf.create_loaded_file(job_name, interface_name)
        session['loaded_file'] = loaded_file
        
        log(f"✅ Created Loaded File: {loaded_file['id']}", "SUCCESS")
        log(f"📎 View: {loaded_file['view_url']}", "INFO")
        
        # Step 2: Create Batch records
        log("", "INFO")
        log("=" * 50, "INFO")
        log(f"📝 STEP 2: Creating {len(json_data)} Batch Records", "INFO")
        
        created_batches = []
        batch_num = 0
        
        for key, data in json_data.items():
            batch_num += 1
            try:
                batch = sf.create_batch_record(loaded_file['id'], data, batch_num)
                created_batches.append(batch)
                log(f"   ✅ Batch {batch_num} ({key}): {batch['id']}", "SUCCESS")
            except Exception as e:
                log(f"   ❌ Batch {batch_num} ({key}) failed: {e}", "ERROR")
                raise
        
        session['batches'] = created_batches
        log(f"✅ Created {len(created_batches)} batch records", "SUCCESS")
        
        # Step 3: Launch Job
        log("", "INFO")
        log("=" * 50, "INFO")
        log("🚀 STEP 3: Launching Job", "INFO")
        
        try:
            launch_result = sf.launch_job(loaded_file['id'])
            log(f"✅ Job launched successfully via {launch_result['method']}", "SUCCESS")
            session['launched'] = True
        except Exception as e:
            log(f"⚠️ Auto-launch failed: {e}", "WARNING")
            log(f"💡 Please launch manually: {loaded_file['view_url']}", "INFO")
            session['launched'] = False
        
        # Step 4: Wait for completion (if launched)
        if session.get('launched'):
            log("", "INFO")
            log("=" * 50, "INFO")
            log("⏳ STEP 4: Waiting for Completion", "INFO")
            
            def status_callback(msg):
                log(f"   {msg}", "INFO")
            
            completion = sf.wait_for_completion(
                loaded_file['id'], 
                timeout=300, 
                poll_interval=10,
                log_callback=status_callback
            )
            
            session['completion'] = completion
            
            if completion['success']:
                log(f"✅ Job completed successfully! Status: {completion['status']}", "SUCCESS")
            else:
                log(f"❌ Job ended with status: {completion['status']}", "ERROR")
        
        # Summary
        log("", "INFO")
        log("=" * 50, "INFO")
        log("📊 SUMMARY", "INFO")
        log(f"   Loaded File: {loaded_file['id']}", "INFO")
        log(f"   Batches Created: {len(created_batches)}", "INFO")
        log(f"   View in Salesforce: {loaded_file['view_url']}", "INFO")
        
        duration = datetime.now() - session['start_time']
        log(f"   Total Time: {duration}", "INFO")
        log("", "INFO")
        log("🎉 Workflow completed!", "SUCCESS")
        
        session['success'] = True
        
    except Exception as e:
        log(f"❌ Critical error: {str(e)}", "ERROR")
        import traceback
        log(f"Stack trace: {traceback.format_exc()}", "ERROR")
        session['success'] = False
        
    finally:
        session['running'] = False


# ============================================================
# FLASK ROUTES
# ============================================================

@app.route('/')
@requires_auth
def index():
    """Serve the main UI"""
    return render_template('index_api.html')


@app.route('/run-test', methods=['POST'])
@requires_auth
def run_test():
    """Start a test run"""
    try:
        # Parse request
        if request.is_json:
            test_data = request.json
        else:
            test_data = {
                'interfaceName': request.form.get('interfaceName'),
                'loadedFileName': request.form.get('loadedFileName'),
                'fileType': request.form.get('fileType', 'json'),
            }
            
            file_type = test_data['fileType']
            
            if file_type == 'excel':
                if 'excelFile' not in request.files:
                    return jsonify({'success': False, 'error': 'Excel file required'})
                excel_file = request.files['excelFile']
                if excel_file.filename == '':
                    return jsonify({'success': False, 'error': 'No Excel file selected'})
                test_data['fileContent'] = excel_file.read()
            else:
                if 'jsonFile' not in request.files:
                    return jsonify({'success': False, 'error': 'JSON file required'})
                json_file = request.files['jsonFile']
                if json_file.filename == '':
                    return jsonify({'success': False, 'error': 'No JSON file selected'})
                try:
                    test_data['jsonData'] = json.loads(json_file.read().decode('utf-8'))
                except json.JSONDecodeError:
                    return jsonify({'success': False, 'error': 'Invalid JSON file'})
        
        # Validate
        if not test_data.get('interfaceName'):
            return jsonify({'success': False, 'error': 'Interface name required'})
        
        if not os.getenv('SALESFORCE_CLIENT_ID') or not os.getenv('SALESFORCE_USERNAME'):
            return jsonify({'success': False, 'error': 'Salesforce JWT not configured in environment'})
        
        # Create session
        session_id = str(uuid.uuid4())
        test_sessions[session_id] = {
            'running': False,
            'logs': [],
            'start_time': None,
            'created_at': datetime.now(),
            'success': None,
            'loaded_file': None,
            'batches': [],
            'launched': False,
            'completion': None
        }
        
        # Start workflow in background
        thread = threading.Thread(target=run_api_workflow, args=(test_data, session_id))
        thread.daemon = True
        thread.start()
        
        return jsonify({
            'success': True,
            'message': 'DLP Test Automation started (API Mode - No Browser!)',
            'session_id': session_id
        })
        
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)})


@app.route('/status/<session_id>')
@requires_auth
def get_status(session_id):
    """Get status of a test session"""
    if session_id not in test_sessions:
        return jsonify({'error': 'Session not found'}), 404
    
    session = test_sessions[session_id].copy()
    
    # Convert datetime for JSON
    if session.get('created_at'):
        session['created_at'] = session['created_at'].isoformat()
    if session.get('start_time'):
        session['start_time'] = session['start_time'].isoformat()
    
    return jsonify(session)


@app.route('/reset/<session_id>', methods=['POST'])
@requires_auth
def reset_session(session_id):
    """Reset/delete a test session"""
    if session_id in test_sessions:
        test_sessions[session_id]['running'] = False
        del test_sessions[session_id]
    return jsonify({'success': True, 'message': 'Session reset'})


@app.route('/health')
def health():
    """Health check endpoint"""
    return jsonify({
        'status': 'healthy',
        'mode': 'API (No Playwright)',
        'active_sessions': len(test_sessions)
    })


@app.route('/json-generator')
@requires_auth
def json_generator():
    """Serve the JSON Generator UI"""
    return render_template('json-generator.html')
# ============================================================
# MAIN
# ============================================================

if __name__ == '__main__':
    print()
    print("=" * 60)
    print("   DLP TEST AUTOMATION - API EDITION")
    print("   No Playwright! No Browser! Just API calls!")
    print("=" * 60)
    print()
    
    # Check configuration
    checks = [
        ('SALESFORCE_CLIENT_ID', os.getenv('SALESFORCE_CLIENT_ID')),
        ('SALESFORCE_USERNAME', os.getenv('SALESFORCE_USERNAME')),
        ('SALESFORCE_PRIVATE_KEY_PATH', os.getenv('SALESFORCE_PRIVATE_KEY_PATH', 'private.key')),
        ('APP_USER', os.getenv('APP_USER')),
        ('APP_PASS', os.getenv('APP_PASS')),
    ]
    
    print("Configuration:")
    for name, value in checks:
        status = "✅" if value else "❌"
        display = "[SET]" if value else "[MISSING]"
        print(f"   {status} {name}: {display}")
    
    print()
    print("=" * 60)
    
    port = int(os.environ.get('PORT', 8000))
    print(f"🚀 Starting server on http://localhost:{port}")
    print("=" * 60)
    print()
    
    app.run(debug=False, host='0.0.0.0', port=port)