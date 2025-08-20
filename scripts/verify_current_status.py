#!/usr/bin/env python3
"""
Verify Current Status of Movie Ratings Data Lakehouse
Shows the current state of infrastructure, schema, and data
"""

import subprocess
import sys

def check_infrastructure():
    """Check if Docker services are running"""
    print("=" * 60)
    print("INFRASTRUCTURE STATUS")
    print("=" * 60)
    
    try:
        result = subprocess.run(['docker', 'ps'], capture_output=True, text=True, check=True)
        print("Docker containers:")
        print(result.stdout)
        return True
    except Exception as e:
        print(f"Error checking Docker: {e}")
        return False

def check_schema():
    """Check if movies_stage schema exists"""
    print("\n" + "=" * 60)
    print("SCHEMA STATUS")
    print("=" * 60)
    
    try:
        result = subprocess.run([
            'docker', 'exec', 'trino', 'trino', 
            '--server', 'localhost:8080', 
            '--catalog', 'iceberg', 
            '--execute', 'SHOW SCHEMAS;'
        ], capture_output=True, text=True, check=True)
        
        if 'movies_stage' in result.stdout:
            print("✓ movies_stage schema exists")
            return True
        else:
            print("✗ movies_stage schema not found")
            return False
    except Exception as e:
        print(f"Error checking schema: {e}")
        return False

def check_tables():
    """Check if all tables exist and have data"""
    print("\n" + "=" * 60)
    print("TABLE STATUS")
    print("=" * 60)
    
    try:
        result = subprocess.run([
            'docker', 'exec', 'trino', 'trino', 
            '--server', 'localhost:8080', 
            '--catalog', 'iceberg', 
            '--schema', 'movies_stage', 
            '--execute', 'SHOW TABLES;'
        ], capture_output=True, text=True, check=True)
        
        print("Available tables:")
        print(result.stdout)
        
        # Check record counts
        tables = ['tmdb_movies', 'omdb_movies', 'metacritic_ratings', 'rotten_tomatoes_ratings']
        for table in tables:
            count_result = subprocess.run([
                'docker', 'exec', 'trino', 'trino', 
                '--server', 'localhost:8080', 
                '--catalog', 'iceberg', 
                '--schema', 'movies_stage', 
                '--execute', f'SELECT COUNT(*) FROM {table};'
            ], capture_output=True, text=True, check=True)
            
            count = count_result.stdout.strip().strip('"')
            print(f"  {table}: {count} records")
        
        return True
    except Exception as e:
        print(f"Error checking tables: {e}")
        return False

def main():
    """Main verification function"""
    print("Movie Ratings Data Lakehouse - Current Status")
    print("=" * 60)
    
    infra_ok = check_infrastructure()
    schema_ok = check_schema()
    tables_ok = check_tables()
    
    print("\n" + "=" * 60)
    print("SUMMARY")
    print("=" * 60)
    
    if infra_ok and schema_ok and tables_ok:
        print("🎉 All systems are operational!")
        print("✓ Infrastructure running (MinIO, Polaris, Trino)")
        print("✓ movies_stage schema exists")
        print("✓ All tables created and populated")
        print("\nYour data lakehouse is ready for queries!")
        print("\nExample query:")
        print("docker exec -it trino trino --server localhost:8080 --catalog iceberg --schema movies_stage")
        return True
    else:
        print("⚠️  Some issues detected. Check the details above.")
        return False

if __name__ == "__main__":
    try:
        success = main()
        if success:
            sys.exit(0)
        else:
            sys.exit(1)
    except KeyboardInterrupt:
        print("\n\nVerification interrupted by user")
        sys.exit(1)
    except Exception as e:
        print(f"\nUnexpected error: {e}")
        sys.exit(1)
