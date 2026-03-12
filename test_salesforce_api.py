"""
Simple Test Script - Create Loaded File + Batch + Launch
Tests the direct API approach for DLP Test Automation
"""

import requests
import json
import jwt
import time
import os
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()


class SalesforceAPITest:
    """Simple test class for Salesforce API operations"""
    
    def __init__(self):
        self.client_id = os.getenv('SALESFORCE_CLIENT_ID')
        self.username = os.getenv('SALESFORCE_USERNAME')
        self.private_key_path = os.getenv('SALESFORCE_PRIVATE_KEY_PATH', 'private.key')
        self.login_url = os.getenv('SALESFORCE_LOGIN_URL', 'https://test.salesforce.com')
        self.org_url = os.getenv('SALESFORCE_ORG_URL', '')
        
        self.access_token = None
        self.instance_url = None
        
    def authenticate(self):
        """Authenticate using JWT Bearer Token"""
        print("=" * 60)
        print("🔐 STEP 1: Authenticating with Salesforce")
        print("=" * 60)
        
        # Read private key
        with open(self.private_key_path, 'r') as f:
            private_key = f.read()
        
        # Create JWT
        payload = {
            'iss': self.client_id,
            'sub': self.username,
            'aud': self.login_url,
            'exp': int(time.time()) + 300
        }
        
        encoded_jwt = jwt.encode(payload, private_key, algorithm='RS256')
        
        # Get access token
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
        
        print(f"✅ Authenticated successfully!")
        print(f"📍 Instance URL: {self.instance_url}")
        print()
        
    def _headers(self):
        return {
            'Authorization': f'Bearer {self.access_token}',
            'Content-Type': 'application/json'
        }
    
    def create_loaded_file(self, interface_name: str, job_name: str = None):
        """
        Create a Transaction_Log__c (Loaded File) record
        """
        print("=" * 60)
        print("📁 STEP 2: Creating Loaded File (Transaction_Log__c)")
        print("=" * 60)
        
        if not job_name:
            timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            job_name = f"API Test : {timestamp}"
        
        # Build record data
        record_data = {
            'Name': job_name,
            'Interface_Name__c': interface_name
        }
        
        print(f"   Job Name: {job_name}")
        print(f"   Interface: {interface_name}")
        
        # Create record via REST API
        url = f"{self.instance_url}/services/data/v59.0/sobjects/Transaction_Log__c"
        response = requests.post(url, headers=self._headers(), json=record_data)
        
        if response.status_code not in [200, 201]:
            print(f"❌ Failed to create Loaded File: {response.text}")
            raise Exception(f"Create failed: {response.text}")
        
        result = response.json()
        record_id = result['id']
        
        view_url = f"{self.org_url}/lightning/r/Transaction_Log__c/{record_id}/view"
        
        print(f"✅ Created Loaded File!")
        print(f"   Record ID: {record_id}")
        print(f"   View URL: {view_url}")
        print()
        
        return {
            'id': record_id,
            'name': job_name,
            'view_url': view_url
        }
    
    def create_batch_record(self, loaded_file_id: str, request_json: dict, batch_number: int = 1):
        """
        Create a Detail_Transaction_log__c (Interface Batch) record
        """
        print("=" * 60)
        print("📝 STEP 3: Creating Interface Batch (Detail_Transaction_log__c)")
        print("=" * 60)
        
        # Convert JSON to string
        json_string = json.dumps(request_json, indent=2)
        
        print(f"   Parent Loaded File: {loaded_file_id}")
        print(f"   JSON Data Length: {len(json_string)} characters")
        
        # Build record data
        # Required fields: Transaction_Log__c (lookup), Error__c, Status__c
        record_data = {
            'Transaction_Log__c': loaded_file_id,  # Lookup to parent
            'Request_JSON__c': json_string,         # The JSON data
            'Error__c': '',                         # Required - empty string
            'Status__c': 'Queued',                  # Required - initial status
            'Batch_Number__c': batch_number         # Optional batch number
        }
        
        # Create record via REST API
        url = f"{self.instance_url}/services/data/v59.0/sobjects/Detail_Transaction_log__c"
        response = requests.post(url, headers=self._headers(), json=record_data)
        
        if response.status_code not in [200, 201]:
            print(f"❌ Failed to create Batch record: {response.text}")
            raise Exception(f"Create failed: {response.text}")
        
        result = response.json()
        record_id = result['id']
        
        view_url = f"{self.org_url}/lightning/r/Detail_Transaction_log__c/{record_id}/view"
        
        print(f"✅ Created Interface Batch!")
        print(f"   Record ID: {record_id}")
        print(f"   View URL: {view_url}")
        print()
        
        return {
            'id': record_id,
            'view_url': view_url
        }
    
    def launch_job(self, loaded_file_id: str):
        """
        Launch the job by calling ErrorRecoveryController.launchTalendJob
        
        Since @AuraEnabled methods can't be called directly via REST,
        we need to use Apex REST or a workaround.
        """
        print("=" * 60)
        print("🚀 STEP 4: Launching Job")
        print("=" * 60)
        
        # Option 1: Try calling via Apex REST endpoint (if one exists)
        # The org might have /services/apexrest/launchTalendJob endpoint
        
        apex_rest_url = f"{self.instance_url}/services/apexrest/launchTalendJob"
        
        try:
            response = requests.post(
                apex_rest_url,
                headers=self._headers(),
                json={'recordId': loaded_file_id}
            )
            
            if response.status_code in [200, 201, 204]:
                print(f"✅ Job launched via Apex REST!")
                return {'success': True, 'method': 'apex_rest'}
        except:
            pass
        
        # Option 2: Use Tooling API to execute anonymous Apex
        print("   Attempting to execute Apex via Tooling API...")
        
        apex_code = f"""
            ErrorRecoveryController.launchTalendJob('{loaded_file_id}');
        """
        
        tooling_url = f"{self.instance_url}/services/data/v59.0/tooling/executeAnonymous"
        response = requests.get(
            tooling_url,
            headers=self._headers(),
            params={'anonymousBody': apex_code}
        )
        
        if response.status_code == 200:
            result = response.json()
            if result.get('success'):
                print(f"✅ Job launched via Anonymous Apex!")
                return {'success': True, 'method': 'anonymous_apex'}
            else:
                print(f"⚠️ Apex execution issue: {result}")
        
        # If we get here, provide manual instructions
        print()
        print("⚠️ Automatic launch not available. Please launch manually:")
        print(f"   1. Go to: {self.org_url}/lightning/r/Transaction_Log__c/{loaded_file_id}/view")
        print(f"   2. Click the 'Launch' button")
        print()
        
        return {'success': False, 'method': 'manual_required'}
    
    def check_status(self, loaded_file_id: str):
        """Check the status of a Loaded File"""
        query = f"SELECT Id, Name, Status__c, Interface_Name__c FROM Transaction_Log__c WHERE Id = '{loaded_file_id}'"
        
        url = f"{self.instance_url}/services/data/v59.0/query"
        response = requests.get(url, headers=self._headers(), params={'q': query})
        
        if response.status_code == 200:
            records = response.json()['records']
            if records:
                return records[0]
        return None
    
    def wait_for_completion(self, loaded_file_id: str, timeout: int = 300):
        """Wait for job to complete"""
        print("=" * 60)
        print("⏳ STEP 5: Waiting for Completion")
        print("=" * 60)
        
        start = time.time()
        
        while time.time() - start < timeout:
            status_record = self.check_status(loaded_file_id)
            
            if status_record:
                status = status_record.get('Status__c', 'Unknown')
                elapsed = int(time.time() - start)
                print(f"   [{elapsed}s] Status: {status}")
                
                # Check for completion
                if 'Completed' in str(status) or 'Heroku Completed' in str(status):
                    print(f"✅ Job completed successfully!")
                    return {'success': True, 'status': status}
                
                # Check for failure
                if 'Failed' in str(status):
                    print(f"❌ Job failed: {status}")
                    return {'success': False, 'status': status}
            
            time.sleep(10)
        
        print(f"⏰ Timeout waiting for completion")
        return {'success': False, 'status': 'Timeout'}


def main():
    """
    Main test function
    """
    print()
    print("=" * 60)
    print("   DLP TEST AUTOMATION - DIRECT API TEST")
    print("=" * 60)
    print()
    
    # Check environment variables
    required_vars = ['SALESFORCE_CLIENT_ID', 'SALESFORCE_USERNAME']
    missing = [v for v in required_vars if not os.getenv(v)]
    
    if missing:
        print(f"❌ Missing environment variables: {', '.join(missing)}")
        print()
        print("Please set:")
        print("  export SALESFORCE_CLIENT_ID='your_client_id'")
        print("  export SALESFORCE_USERNAME='your_username'")
        print("  export SALESFORCE_PRIVATE_KEY_PATH='path/to/private.key'")
        return
    
    # Test data
    interface_name = "CAP_YOU_CI_INTERFACE"
    
    request_json = {
        "doryList": [
            {
                "conTs": "2025-09-09T03:00:33.000Z",
                "consent": "1",
                "fname": "Test1",
                "lname": "REQ-6475",
                "email": "test1@req6475.com",
                "colCoun": "DE",
                "colLang": "G",
                "doarr": "2025-09-10",
                "dodep": "2025-09-11",
                "swid": "",
                "colAccom": "",
                "bookingId": "",
                "roomNb": "",
                "roomType": "",
                "roomClass": "",
                "dconsent": "1",
                "dconTs": "2025-09-10T02:25:26.000Z",
                "dconIp": "80.187.115.111",
                "colCampaign": "",
                "source": "OLCI",
                "sourceGroup": "WEB",
                "ciId": ""
            }
        ]
    }
    
    try:
        # Create test instance
        sf = SalesforceAPITest()
        
        # Step 1: Authenticate
        sf.authenticate()
        
        # Step 2: Create Loaded File
        loaded_file = sf.create_loaded_file(interface_name)
        
        # Step 3: Create Batch record with JSON
        batch = sf.create_batch_record(loaded_file['id'], request_json)
        
        # Step 4: Launch the job
        launch_result = sf.launch_job(loaded_file['id'])
        
        # Step 5: If launched, wait for completion
        if launch_result.get('success'):
            completion = sf.wait_for_completion(loaded_file['id'], timeout=120)
        
        # Summary
        print()
        print("=" * 60)
        print("   SUMMARY")
        print("=" * 60)
        print(f"   Loaded File ID: {loaded_file['id']}")
        print(f"   Batch Record ID: {batch['id']}")
        print(f"   View Loaded File: {loaded_file['view_url']}")
        print()
        
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()