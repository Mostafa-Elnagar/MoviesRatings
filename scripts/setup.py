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


class MovieRatingsSetup:
    """Complete setup for Movie Ratings data lakehouse"""
    
    def __init__(self):
        self.base_url = "http://localhost:8181"
        self.access_token: Optional[str] = None
        
    def start_infrastructure(self) -> bool:
        """Start Docker services"""
        print("Starting Docker services...")
        try:
            subprocess.run(['docker-compose', '-f', 'infrastructure/docker-compose.yml', 'up', '-d'], check=True)
            print("Services started successfully")
            return True
        except subprocess.CalledProcessError as e:
            print(f"Failed to start services: {e}")
            return False
    
    def wait_for_services(self, timeout: int = 180) -> bool:
        """Wait for all services to be ready"""
        print("Waiting for services to be ready...")
        
        start_time = time.time()
        services_ready = {'minio': False, 'polaris': False, 'trino': False}
        
        while time.time() - start_time < timeout:
            # Check MinIO
            if not services_ready['minio']:
                try:
                    subprocess.run(['docker', 'exec', 'minio-client', 'mc', 'ls', 'minio'], 
                                 capture_output=True, text=True, check=True, timeout=10)
                    print("MinIO is ready")
                    services_ready['minio'] = True
                except:
                    pass
            
            # Check Polaris
            if not services_ready['polaris']:
                try:
                    response = requests.get(f"{self.base_url}/api/catalog/", timeout=5)
                    if response.status_code in [200, 404]:
                        print("Polaris is ready")
                        services_ready['polaris'] = True
                except:
                    pass
            
            # Check Trino
            if not services_ready['trino']:
                try:
                    subprocess.run(['docker', 'exec', 'trino', 'trino', '--server', 'localhost:8080', '--execute', 'SELECT 1;'], 
                                 capture_output=True, text=True, check=True, timeout=10)
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
        
        url = f"{self.base_url}/api/catalog/v1/oauth/tokens"
        data = {
            'grant_type': 'client_credentials',
            'client_id': 'root',
            'client_secret': 'secret',
            'scope': 'PRINCIPAL_ROLE:ALL'
        }
        
        try:
            response = requests.post(url, data=data, timeout=10)
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
        
        url = f"{self.base_url}/api/management/v1/catalogs"
        headers = {"Authorization": f"Bearer {self.access_token}"}
        
        catalog_config = {
            "name": "polariscatalog",
            "type": "INTERNAL",
            "properties": {
                "default-base-location": "s3://warehouse",
                "s3.endpoint": "http://minio:9000",
                "s3.path-style-access": "true",
                "s3.access-key-id": "minioadmin",
                "s3.secret-access-key": "minioadmin",
                "s3.region": "dummy-region"
            },
            "storageConfigInfo": {
                "roleArn": "arn:aws:iam::000000000000:role/minio-polaris-role",
                "storageType": "S3",
                "allowedLocations": ["s3://warehouse/*"]
            }
        }
        
        try:
            response = requests.post(url, headers=headers, json=catalog_config, timeout=10)
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
        url = f"{self.base_url}/api/management/v1/catalogs/polariscatalog/catalog-roles/catalog_admin/grants"
        grant_data = {"grant": {"type": "catalog", "privilege": "CATALOG_MANAGE_CONTENT"}}
        
        try:
            response = requests.put(url, headers=headers, json=grant_data, timeout=10)
            if response.status_code not in [200, 201]:
                print(f"Failed to create catalog admin role: {response.status_code}")
                return False
        except Exception as e:
            print(f"Error creating catalog admin role: {e}")
            return False
        
        # Create data engineer role
        url = f"{self.base_url}/api/management/v1/principal-roles"
        role_data = {"principalRole": {"name": "data_engineer"}}
        
        try:
            response = requests.post(url, headers=headers, json=role_data, timeout=10)
            if response.status_code not in [200, 201]:
                print(f"Failed to create data engineer role: {response.status_code}")
                return False
        except Exception as e:
            print(f"Error creating data engineer role: {e}")
            return False
        
        # Connect the roles
        url = f"{self.base_url}/api/management/v1/principal-roles/data_engineer/catalog-roles/polariscatalog"
        catalog_role_data = {"catalogRole": {"name": "catalog_admin"}}
        
        try:
            response = requests.put(url, headers=headers, json=catalog_role_data, timeout=10)
            if response.status_code not in [200, 201]:
                print(f"Failed to connect roles: {response.status_code}")
                return False
        except Exception as e:
            print(f"Error connecting roles: {e}")
            return False
        
        # Give root the data engineer role
        url = f"{self.base_url}/api/management/v1/principals/root/principal-roles"
        principal_role_data = {"principalRole": {"name": "data_engineer"}}
        
        try:
            response = requests.put(url, headers=headers, json=principal_role_data, timeout=10)
            if response.status_code not in [200, 201]:
                print(f"Failed to assign role to root: {response.status_code}")
                return False
        except Exception as e:
            print(f"Error assigning role to root: {e}")
            return False
        
        print("Permissions setup completed")
        return True
    
    def create_tables(self) -> bool:
        """Create Iceberg tables"""
        print("Creating Iceberg tables...")
        
        # Create schema first
        schema_cmd = "CREATE SCHEMA IF NOT EXISTS iceberg.movies;"
        if not self._execute_trino_command(schema_cmd, "Creating movies schema"):
            return False
        
        # Table definitions
        tables = {
            'raw_movies': """
                CREATE TABLE IF NOT EXISTS iceberg.movies.raw_movies (
                    id BIGINT, title VARCHAR, release_date DATE, overview VARCHAR,
                    popularity DOUBLE, vote_average DOUBLE, vote_count INTEGER,
                    genre_ids ARRAY(INTEGER), original_language VARCHAR,
                    original_title VARCHAR, backdrop_path VARCHAR, poster_path VARCHAR,
                    created_at TIMESTAMP, updated_at TIMESTAMP
                ) WITH (
                    format = 'PARQUET',
                    partitioning = ARRAY['year(release_date)']
                )
            """,
            'enriched_movies': """
                CREATE TABLE IF NOT EXISTS iceberg.movies.enriched_movies (
                    id BIGINT, title VARCHAR, release_date DATE, overview VARCHAR,
                    popularity DOUBLE, vote_average DOUBLE, vote_count INTEGER,
                    genres ARRAY(VARCHAR), original_language VARCHAR,
                    original_title VARCHAR, backdrop_path VARCHAR, poster_path VARCHAR,
                    imdb_rating DOUBLE, imdb_votes INTEGER, metacritic_score INTEGER,
                    rotten_tomatoes_score INTEGER, created_at TIMESTAMP, updated_at TIMESTAMP
                ) WITH (
                    format = 'PARQUET',
                    partitioning = ARRAY['year(release_date)']
                )
            """,
            'movie_ratings': """
                CREATE TABLE IF NOT EXISTS iceberg.movies.movie_ratings (
                    movie_id BIGINT, source VARCHAR, rating DOUBLE, max_rating DOUBLE,
                    votes_count INTEGER, rating_date DATE, created_at TIMESTAMP
                ) WITH (
                    format = 'PARQUET',
                    partitioning = ARRAY['year(rating_date)']
                )
            """
        }
        
        # Create each table
        for table_name, create_sql in tables.items():
            if not self._execute_trino_command(create_sql, f"Creating {table_name} table"):
                return False
        
        print("All tables created successfully")
        return True
    
    def _execute_trino_command(self, command: str, description: str) -> bool:
        """Execute a Trino command"""
        print(f"Executing: {description}")
        
        try:
            subprocess.run([
                'docker', 'exec', 'trino', 'trino',
                '--server', 'localhost:8080',
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
            result = subprocess.run([
                'docker', 'exec', 'trino', 'trino',
                '--server', 'localhost:8080',
                '--catalog', 'iceberg',
                '--schema', 'movies',
                '--execute', 'SHOW TABLES;'
            ], capture_output=True, text=True, check=True)
            
            if 'raw_movies' in result.stdout and 'enriched_movies' in result.stdout:
                print("Setup verification successful!")
                print("Available tables:")
                print(result.stdout)
                return True
            else:
                print("Setup verification failed - tables not found")
                return False
                
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
        
        if not self.create_tables():
            print("Table creation failed")
            return False
        
        if not self.verify_setup():
            print("Setup verification failed")
            return False
        
        print("\n" + "=" * 50)
        print("Setup completed successfully!")
        print("Your Movie Ratings data lakehouse is ready!")
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
