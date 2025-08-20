#!/usr/bin/env python3
"""
Complete Movie Ratings Data Lakehouse Pipeline
1. Setup infrastructure (Docker services)
2. Ingest data from APIs and scrapers
3. Create movies_stage schema and tables
4. Load staged data into the data lakehouse
"""

import os
import sys
import time
from pathlib import Path
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Add project root to path for imports
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

def run_setup():
    """Run the infrastructure setup"""
    print("=" * 60)
    print("STEP 1: Setting up infrastructure...")
    print("=" * 60)
    
    try:
        import subprocess
        result = subprocess.run(['python', 'scripts/setup.py'], 
                              capture_output=True, text=True, check=True)
        print("Infrastructure setup completed successfully!")
        return True
    except subprocess.CalledProcessError as e:
        print(f"✗ Infrastructure setup failed: {e}")
        if e.stderr:
            print(f"Error details: {e.stderr}")
        return False
    except Exception as e:
        print(f"✗ Infrastructure setup failed: {e}")
        return False

def run_data_ingestion():
    """Run the data ingestion pipeline"""
    print("\n" + "=" * 60)
    print("STEP 2: Ingesting data from APIs and scrapers...")
    print("=" * 60)
    
    try:
        # Sample IMDB IDs for testing (top movies)
        sample_imdb_ids = [
            "tt0111161",  # The Shawshank Redemption
            "tt0068646",  # The Godfather
            "tt0468569",  # The Dark Knight
            "tt0071562",  # The Godfather Part II
            "tt0050083",  # 12 Angry Men
            "tt0108052",  # Schindler's List
            "tt0167260",  # The Lord of the Rings: The Return of the King
            "tt0110912",  # Pulp Fiction
            "tt0133093",  # The Matrix
            "tt0060196"   # The Good, the Bad and the Ugly
        ]
        
        from data_pipeline.comprehensive_ingestor import ComprehensiveMovieIngestor
        
        ingestor = ComprehensiveMovieIngestor()
        success = ingestor.run_ingestion_pipeline(sample_imdb_ids, max_movies=10)
        
        if success:
            print("Data ingestion completed successfully!")
            return True
        else:
            print("✗ Data ingestion failed!")
            return False
            
    except Exception as e:
        print(f"✗ Data ingestion failed: {e}")
        return False

def run_schema_creation():
    """Create the movies_stage schema and tables"""
    print("\n" + "=" * 60)
    print("STEP 3: Creating movies_stage schema and tables...")
    print("=" * 60)
    
    try:
        import subprocess
        result = subprocess.run(['python', 'scripts/create_stage_schema.py'], 
                              capture_output=True, text=True, check=True)
        print("Schema and tables creation completed successfully!")
        return True
    except subprocess.CalledProcessError as e:
        print(f"✗ Schema creation failed: {e}")
        if e.stderr:
            print(f"Error details: {e.stderr}")
        return False
    except Exception as e:
        print(f"✗ Schema creation failed: {e}")
        return False

def run_data_loading():
    """Load staged data into the data lakehouse"""
    print("\n" + "=" * 60)
    print("STEP 4: Loading staged data into data lakehouse...")
    print("=" * 60)
    
    try:
        from data_pipeline.stage_data_loader import StageDataLoader
        
        loader = StageDataLoader()
        success = loader.load_all_staged_data()
        
        if success:
            print("Data loading completed successfully!")
            # Verify the data
            loader.verify_data_loaded()
            return True
        else:
            print("✗ Data loading failed!")
            return False
            
    except Exception as e:
        print(f"✗ Data loading failed: {e}")
        return False

def main():
    """Run the complete pipeline"""
    print("Movie Ratings Data Lakehouse - Complete Pipeline")
    print("=" * 60)
    print("This pipeline will:")
    print("1. Setup Docker infrastructure (MinIO, Polaris, Trino)")
    print("2. Ingest movie data from TMDB, OMDb, Metacritic, and Rotten Tomatoes")
    print("3. Create movies_stage schema with optimized tables")
    print("4. Load all staged data into the data lakehouse")
    print("=" * 60)
    
    # Check if .env file exists
    if not os.path.exists('.env'):
        print("✗ Error: .env file not found!")
        print("Please create a .env file with your API keys:")
        print("TMDB_API_KEY=your_tmdb_api_key_here")
        print("OMDB_API_KEY=your_omdb_api_key_here")
        return False
    
    # Check if API keys are set
    tmdb_key = os.getenv('TMDB_API_KEY')
    omdb_key = os.getenv('OMDB_API_KEY')
    
    if not tmdb_key or not omdb_key:
        print("✗ Error: API keys not found in .env file!")
        print("Please ensure TMDB_API_KEY and OMDB_API_KEY are set")
        return False
    
    print("Environment configuration verified")
    
    # Run the pipeline steps
    start_time = time.time()
    
    # Step 1: Setup infrastructure
    if not run_setup():
        print("\n✗ Pipeline failed at infrastructure setup step")
        return False
    
    # Wait a bit for services to be fully ready
    print("\nWaiting for services to be fully ready...")
    time.sleep(10)
    
    # Step 2: Data ingestion
    if not run_data_ingestion():
        print("\n✗ Pipeline failed at data ingestion step")
        return False
    
    # Step 3: Schema creation
    if not run_schema_creation():
        print("\n✗ Pipeline failed at schema creation step")
        return False
    
    # Step 4: Data loading
    if not run_data_loading():
        print("\n✗ Pipeline failed at data loading step")
        return False
    
    # Pipeline completed successfully
    end_time = time.time()
    duration = end_time - start_time
    
    print("\n" + "=" * 60)
    print("🎉 PIPELINE COMPLETED SUCCESSFULLY! 🎉")
    print("=" * 60)
    print(f"Total execution time: {duration:.2f} seconds")
    print("\nYour Movie Ratings Data Lakehouse is now ready with:")
    print("Infrastructure running (MinIO, Polaris, Trino)")
    print("Data ingested from multiple sources")
    print("movies_stage schema created with optimized tables")
    print("All data loaded and ready for analysis")
    print("\nYou can now query your data using Trino:")
    print("docker exec -it trino trino --server localhost:8080 --catalog iceberg --schema movies_stage")
    print("=" * 60)
    
    return True

if __name__ == "__main__":
    try:
        success = main()
        if success:
            sys.exit(0)
        else:
            sys.exit(1)
    except KeyboardInterrupt:
        print("\n\nPipeline interrupted by user")
        sys.exit(1)
    except Exception as e:
        print(f"\nUnexpected error: {e}")
        sys.exit(1)
