#!/usr/bin/env python3
"""
Simple Status Check Script
Quick check if the Movie Ratings data lakehouse is working
"""

import subprocess
import requests
import sys
from pathlib import Path

# Add project root to path for imports
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from config.settings import *


def check_docker_services():
    """Check if Docker services are running"""
    print("Checking Docker services...")
    try:
        subprocess.run(['docker-compose', '-f', DOCKER_COMPOSE_FILE, 'ps'], 
                      capture_output=True, text=True, check=True)
        print("Docker services are running")
        return True
    except subprocess.CalledProcessError:
        print("Docker services are not running")
        return False


def check_trino():
    """Check if Trino is accessible"""
    print("Checking Trino...")
    try:
        subprocess.run([
            'docker', 'exec', TRINO_CONTAINER, 'trino',
            '--server', TRINO_SERVER,
            '--execute', 'SELECT 1 as test;'
        ], capture_output=True, text=True, check=True)
        print("Trino is accessible")
        return True
    except subprocess.CalledProcessError:
        print("Trino is not accessible")
        return False


def check_polaris():
    """Check if Polaris is accessible"""
    print("Checking Polaris...")
    try:
        response = requests.get(POLARIS_CATALOG_ENDPOINT, timeout=5)
        if response.status_code in [200, 404]:  # 404 is expected for this endpoint
            print("Polaris is accessible")
            return True
        else:
            print(f"Polaris returned status {response.status_code}")
            return False
    except Exception as e:
        print(f"Polaris is not accessible: {e}")
        return False


def check_minio():
    """Check if MinIO is accessible"""
    print("Checking MinIO...")
    try:
        subprocess.run([
            'docker', 'exec', MINIO_CLIENT_CONTAINER, 'mc', 'ls', 'minio'
        ], capture_output=True, text=True, check=True)
        print("MinIO is accessible")
        return True
    except subprocess.CalledProcessError:
        print("MinIO is not accessible")
        return False


def check_tables():
    """Check if Iceberg tables exist"""
    print("Checking Iceberg tables...")
    try:
        result = subprocess.run([
            'docker', 'exec', TRINO_CONTAINER, 'trino',
            '--server', TRINO_SERVER,
            '--catalog', 'iceberg',
            '--schema', ICEBERG_SCHEMA,
            '--execute', 'SHOW TABLES;'
        ], capture_output=True, text=True, check=True)
        
        if RAW_MOVIES_TABLE in result.stdout and ENRICHED_MOVIES_TABLE in result.stdout:
            print("Iceberg tables exist")
            return True
        else:
            print("Iceberg tables not found")
            return False
    except subprocess.CalledProcessError:
        print("Cannot check Iceberg tables")
        return False


def main():
    """Main function"""
    print("Movie Ratings Data Lakehouse Status Check")
    print("=" * 50)
    
    checks = [
        check_docker_services,
        check_trino,
        check_polaris,
        check_minio,
        check_tables
    ]
    
    results = []
    for check in checks:
        results.append(check())
        print()
    
    print("=" * 50)
    if all(results):
        print("All systems are operational!")
        print("Your data lakehouse is ready for use.")
    else:
        print("Some systems have issues.")
        print("Run 'python scripts/setup.py' to fix them.")
    
    return 0 if all(results) else 1


if __name__ == "__main__":
    sys.exit(main())
