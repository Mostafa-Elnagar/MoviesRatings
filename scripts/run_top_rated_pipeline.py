#!/usr/bin/env python3
"""
Run Top-Rated Movies Pipeline
Fetches all top-rated movies from TMDB and enriches with additional data
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

def main():
    """Run the top-rated movies pipeline"""
    print("Top-Rated Movies Ingestion Pipeline")
    print("=" * 60)
    print("This pipeline will:")
    print("1. Fetch top-rated movies from TMDB (page by page)")
    print("2. Get detailed movie information including credits and external IDs")
    print("3. Enrich with OMDb data for movies with IMDB IDs")
    print("4. Scrape Metacritic and Rotten Tomatoes ratings")
    print("5. Save all data to staged files")
    print("=" * 60)
    
    # Check if .env file exists
    if not os.path.exists('.env'):
        print("Error: .env file not found!")
        print("Please create a .env file with your API keys:")
        print("TMDB_API_KEY=your_tmdb_api_key_here")
        print("OMDB_API_KEY=your_omdb_api_key_here")
        return False
    
    # Check if API keys are set
    tmdb_key = os.getenv('TMDB_API_KEY')
    omdb_key = os.getenv('OMDB_API_KEY')
    
    if not tmdb_key or not omdb_key:
        print("Error: API keys not found in .env file!")
        print("Please ensure TMDB_API_KEY and OMDB_API_KEY are set")
        return False
    
    print("Environment configuration verified")
    
    # Get user input for pipeline parameters
    print("\nPipeline Configuration:")
    print("-" * 30)
    
    try:
        max_pages = int(input("Maximum pages to fetch from TMDB (default 50): ") or "50")
        max_omdb = int(input("Maximum OMDb enrichments (default 100): ") or "100")
        max_scraping = int(input("Maximum scraping operations (default 25): ") or "25")
    except ValueError:
        print("Invalid input. Using default values.")
        max_pages = 50
        max_omdb = 100
        max_scraping = 25
    
    print(f"\nPipeline will fetch up to {max_pages} pages ({max_pages * 20} movies)")
    print(f"Enrich up to {max_omdb} movies with OMDb data")
    print(f"Scrape ratings for up to {max_scraping} movies")
    
    # Confirm before starting
    confirm = input("\nProceed with pipeline? (y/N): ").lower().strip()
    if confirm not in ['y', 'yes']:
        print("Pipeline cancelled.")
        return False
    
    # Run the pipeline
    print("\n" + "=" * 60)
    print("STARTING TOP-RATED MOVIES PIPELINE")
    print("=" * 60)
    
    start_time = time.time()
    
    try:
        from data_pipeline.top_rated_ingestor import TopRatedMoviesIngestor
        
        ingestor = TopRatedMoviesIngestor()
        success = ingestor.run_top_rated_pipeline(
            max_pages=max_pages,
            max_omdb=max_omdb,
            max_scraping=max_scraping
        )
        
        if success:
            end_time = time.time()
            duration = end_time - start_time
            
            print("\n" + "=" * 60)
            print("🎉 TOP-RATED MOVIES PIPELINE COMPLETED SUCCESSFULLY! 🎉")
            print("=" * 60)
            print(f"Total execution time: {duration:.2f} seconds")
            print(f"Check the data/raw/ directory for staged data files")
            print("\nNext steps:")
            print("1. Review the staged data files")
            print("2. Load data into the data lakehouse using stage_data_loader.py")
            print("3. Run analytical queries on the new data")
            print("=" * 60)
            return True
        else:
            print("\nPipeline failed. Check the logs for details.")
            return False
            
    except Exception as e:
        print(f"\nUnexpected error: {e}")
        return False

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
