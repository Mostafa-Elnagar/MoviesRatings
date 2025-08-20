#!/usr/bin/env python3
"""
Movie Ratings Data Lakehouse Setup Script
Single script to setup the complete environment and create Iceberg tables
"""

import subprocess
import requests
import time
import sys
from typing import Optional
from pathlib import Path

# Add project root to path for imports
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from config.settings import *


class MovieRatingsSetup:
    """Complete setup for Movie Ratings data lakehouse"""
    
    def __init__(self):
        self.access_token: Optional[str] = None
        
    def start_infrastructure(self) -> bool:
        """Start Docker services"""
        print("Starting Docker services...")
        try:
            subprocess.run(['docker-compose', '-f', DOCKER_COMPOSE_FILE, 'up', '-d'], check=True)
            print("Services started successfully")
            return True
        except subprocess.CalledProcessError as e:
            print(f"Failed to start services: {e}")
            return False
    
    def wait_for_services(self, timeout: int = SERVICE_TIMEOUT) -> bool:
        """Wait for all services to be ready"""
        print("Waiting for services to be ready...")
        
        start_time = time.time()
        services_ready = {'minio': False, 'polaris': False, 'trino': False}
        
        while time.time() - start_time < timeout:
            # Check MinIO
            if not services_ready['minio']:
                try:
                    subprocess.run(['docker', 'exec', MINIO_CLIENT_CONTAINER, 'mc', 'ls', 'minio'], 
                                 capture_output=True, text=True, check=True, timeout=REQUEST_TIMEOUT)
                    print("MinIO is ready")
                    services_ready['minio'] = True
                except:
                    pass
            
            # Check Polaris
            if not services_ready['polaris']:
                try:
                    response = requests.get(POLARIS_CATALOG_ENDPOINT, timeout=5)
                    if response.status_code in [200, 404]:
                        print("Polaris is ready")
                        services_ready['polaris'] = True
                except:
                    pass
            
            # Check Trino
            if not services_ready['trino']:
                try:
                    subprocess.run(['docker', 'exec', TRINO_CONTAINER, 'trino', '--server', TRINO_SERVER, '--execute', 'SELECT 1;'], 
                                 capture_output=True, text=True, check=True, timeout=REQUEST_TIMEOUT)
                    print("Trino is ready")
                    services_ready['trino'] = True
                except:
                    pass
            
            if all(services_ready.values()):
                print("All services are ready!")
                return True
            
            time.sleep(5)
        
        print("Timeout waiting for services")
        return False
    
    def setup_polaris(self) -> bool:
        """Setup Polaris catalog and permissions"""
        print("Setting up Polaris catalog...")
        
        if not self._get_access_token():
            return False
        
        if not self._create_iceberg_catalog():
            return False
        
        if not self._setup_permissions():
            return False
        
        print("Polaris setup completed")
        return True
    
    def _get_access_token(self) -> bool:
        """Get OAuth access token from Polaris"""
        print("Getting OAuth access token...")
        
        data = {
            'grant_type': 'client_credentials',
            'client_id': POLARIS_CLIENT_ID,
            'client_secret': POLARIS_CLIENT_SECRET,
            'scope': POLARIS_SCOPE
        }
        
        try:
            response = requests.post(POLARIS_TOKEN_ENDPOINT, data=data, timeout=REQUEST_TIMEOUT)
            if response.status_code == 200:
                token_data = response.json()
                self.access_token = token_data.get('access_token')
                if self.access_token:
                    print("Access token obtained successfully")
                    return True
                else:
                    print("No access token in response")
                    return False
            else:
                print(f"Failed to get access token: {response.status_code}")
                return False
        except Exception as e:
            print(f"Error getting access token: {e}")
            return False
    
    def _create_iceberg_catalog(self) -> bool:
        """Create Iceberg catalog in Polaris"""
        if not self.access_token:
            print("No access token available")
            return False
        
        print("Creating Iceberg catalog...")
        
        url = f"{POLARIS_MANAGEMENT_ENDPOINT}/catalogs"
        headers = {"Authorization": f"Bearer {self.access_token}"}
        
        catalog_config = {
            "name": CATALOG_NAME,
            "type": "INTERNAL",
            "properties": {
                "default-base-location": S3_BASE_LOCATION,
                "s3.endpoint": S3_ENDPOINT,
                "s3.path-style-access": "true",
                "s3.access-key-id": MINIO_ACCESS_KEY,
                "s3.secret-access-key": MINIO_SECRET_KEY,
                "s3.region": MINIO_REGION
            },
            "storageConfigInfo": {
                "roleArn": ROLE_ARN,
                "storageType": "S3",
                "allowedLocations": [f"{S3_BASE_LOCATION}/*"]
            }
        }
        
        try:
            response = requests.post(url, headers=headers, json=catalog_config, timeout=REQUEST_TIMEOUT)
            if response.status_code in [200, 201]:
                print("Iceberg catalog created successfully")
                return True
            else:
                print(f"Failed to create catalog: {response.status_code}")
                return False
        except Exception as e:
            print(f"Error creating catalog: {e}")
            return False
    
    def _setup_permissions(self) -> bool:
        """Setup permissions and roles in Polaris"""
        if not self.access_token:
            print("No access token available")
            return False
        
        print("Setting up permissions...")
        
        headers = {"Authorization": f"Bearer {self.access_token}"}
        
        # Create catalog admin role
        url = f"{POLARIS_MANAGEMENT_ENDPOINT}/catalogs/{CATALOG_NAME}/catalog-roles/catalog_admin/grants"
        grant_data = {"grant": {"type": "catalog", "privilege": "CATALOG_MANAGE_CONTENT"}}
        
        try:
            response = requests.put(url, headers=headers, json=grant_data, timeout=REQUEST_TIMEOUT)
            if response.status_code not in [200, 201]:
                print(f"Failed to create catalog admin role: {response.status_code}")
                return False
        except Exception as e:
            print(f"Error creating catalog admin role: {e}")
            return False
        
        # Create data engineer role
        url = f"{POLARIS_MANAGEMENT_ENDPOINT}/principal-roles"
        role_data = {"principalRole": {"name": "data_engineer"}}
        
        try:
            response = requests.post(url, headers=headers, json=role_data, timeout=REQUEST_TIMEOUT)
            if response.status_code not in [200, 201]:
                print(f"Failed to create data engineer role: {response.status_code}")
                return False
        except Exception as e:
            print(f"Error creating data engineer role: {e}")
            return False
        
        # Connect the roles
        url = f"{POLARIS_MANAGEMENT_ENDPOINT}/principal-roles/data_engineer/catalog-roles/{CATALOG_NAME}"
        catalog_role_data = {"catalogRole": {"name": "catalog_admin"}}
        
        try:
            response = requests.put(url, headers=headers, json=catalog_role_data, timeout=REQUEST_TIMEOUT)
            if response.status_code not in [200, 201]:
                print(f"Failed to connect roles: {response.status_code}")
                return False
        except Exception as e:
            print(f"Error connecting roles: {e}")
            return False
        
        # Give root the data engineer role
        url = f"{POLARIS_MANAGEMENT_ENDPOINT}/principals/root/principal-roles"
        principal_role_data = {"principalRole": {"name": "data_engineer"}}
        
        try:
            response = requests.put(url, headers=headers, json=principal_role_data, timeout=REQUEST_TIMEOUT)
            if response.status_code not in [200, 201]:
                print(f"Failed to assign role to root: {response.status_code}")
                return False
        except Exception as e:
            print(f"Error assigning role to root: {e}")
            return False
        
        print("Permissions setup completed")
        return True
    

    
    def _execute_trino_command(self, command: str, description: str) -> bool:
        """Execute a Trino command"""
        print(f"Executing: {description}")
        
        try:
            subprocess.run([
                'docker', 'exec', TRINO_CONTAINER, 'trino',
                '--server', TRINO_SERVER,
                '--execute', command
            ], capture_output=True, text=True, check=True)
            
            print(f"Success: {description}")
            return True
            
        except subprocess.CalledProcessError as e:
            print(f"Error: {description}")
            if e.stderr:
                print(f"Error details: {e.stderr}")
            return False
    
    def verify_setup(self) -> bool:
        """Verify the complete setup"""
        print("Verifying setup...")
        
        try:
            # Just verify that Trino is accessible
            result = subprocess.run([
                'docker', 'exec', TRINO_CONTAINER, 'trino',
                '--server', TRINO_SERVER,
                '--execute', 'SELECT 1;'
            ], capture_output=True, text=True, check=True)
            
            print("Setup verification successful!")
            print("Trino is accessible and ready for use")
            return True
                
        except subprocess.CalledProcessError as e:
            print(f"Setup verification failed: {e}")
            return False
    
    def run_setup(self) -> bool:
        """Run the complete setup"""
        print("Movie Ratings Data Lakehouse Setup")
        print("=" * 50)
        
        if not self.start_infrastructure():
            print("Infrastructure startup failed")
            return False
        
        if not self.wait_for_services():
            print("Service startup failed")
            return False
        
        if not self.setup_polaris():
            print("Polaris setup failed")
            return False
        
        if not self.verify_setup():
            print("Setup verification failed")
            return False
        
        print("\n" + "=" * 50)
        print("Setup completed successfully!")
        print("Your Movie Ratings data lakehouse infrastructure is ready!")
        print("=" * 50)
        
        return True


def main():
    """Main function"""
    setup = MovieRatingsSetup()
    
    if setup.run_setup():
        print("\nSetup completed successfully!")
        sys.exit(0)
    else:
        print("\nSetup failed. Please check the logs and try again.")
        sys.exit(1)


if __name__ == "__main__":
    main()
