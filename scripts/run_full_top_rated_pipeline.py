#!/usr/bin/env python3
"""
Full Top-Rated Movies Pipeline Runner
Fetches ALL top-rated movies from TMDB (all 500+ pages)
"""

import os
import sys
import time
import yaml
from pathlib import Path
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Add project root to path for imports
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

def load_config():
    """Load pipeline configuration"""
    config_path = project_root / "config" / "top_rated_pipeline_config.yaml"
    if config_path.exists():
        with open(config_path, 'r') as f:
            return yaml.safe_load(f)
    else:
        # Default configuration
        return {
            'tmdb': {'max_pages': 500, 'movies_per_page': 20, 'rate_limit_delay': 0.25},
            'omdb': {'max_enrichments': 1000, 'rate_limit_delay': 0.1},
            'scraping': {'max_movies': 100, 'metacritic': {'rate_limit_delay': 1.0}, 'rotten_tomatoes': {'rate_limit_delay': 2.0}},
            'data_processing': {'save_intermediate': True, 'batch_size': 100, 'retry_failed': True, 'max_retries': 3},
            'output': {'data_directory': 'data/raw', 'file_prefix': 'tmdb_top_rated', 'include_timestamp': True, 'compression': False}
        }

def main():
    """Run the full top-rated movies pipeline"""
    print("🎬 FULL TOP-RATED MOVIES PIPELINE 🎬")
    print("=" * 70)
    print("This pipeline will fetch ALL top-rated movies from TMDB")
    print("Estimated: 500+ pages = 10,000+ movies")
    print("=" * 70)
    
    # Load configuration
    config = load_config()
    print(f"Configuration loaded: {config['tmdb']['max_pages']} pages, {config['tmdb']['max_pages'] * config['tmdb']['movies_per_page']} movies")
    
    # Check environment
    if not os.path.exists('.env'):
        print("❌ Error: .env file not found!")
        print("Please create a .env file with your API keys:")
        print("TMDB_API_KEY=your_tmdb_api_key_here")
        print("OMDB_API_KEY=your_omdb_api_key_here")
        return False
    
    tmdb_key = os.getenv('TMDB_API_KEY')
    omdb_key = os.getenv('OMDB_API_KEY')
    
    if not tmdb_key or not omdb_key:
        print("❌ Error: API keys not found in .env file!")
        return False
    
    print("✅ Environment configuration verified")
    
    # Pipeline configuration
    print("\n📋 Pipeline Configuration:")
    print("-" * 40)
    
    try:
        max_pages = int(input(f"Maximum pages to fetch (default {config['tmdb']['max_pages']}): ") or str(config['tmdb']['max_pages']))
        max_omdb = int(input(f"Maximum OMDb enrichments (default {config['omdb']['max_enrichments']}): ") or str(config['omdb']['max_enrichments']))
        max_scraping = int(input(f"Maximum scraping operations (default {config['scraping']['max_movies']}): ") or str(config['scraping']['max_movies']))
        
        # Update config with user input
        config['tmdb']['max_pages'] = max_pages
        config['omdb']['max_enrichments'] = max_omdb
        config['scraping']['max_movies'] = max_scraping
        
    except ValueError:
        print("Invalid input. Using default values.")
    
    estimated_movies = config['tmdb']['max_pages'] * config['tmdb']['movies_per_page']
    estimated_time = (config['tmdb']['max_pages'] * 10) + (config['omdb']['max_enrichments'] * 0.1) + (config['scraping']['max_movies'] * 2)
    
    print(f"\n📊 Pipeline Summary:")
    print(f"  • TMDB Pages: {config['tmdb']['max_pages']}")
    print(f"  • Estimated Movies: {estimated_movies:,}")
    print(f"  • OMDb Enrichments: {config['omdb']['max_enrichments']}")
    print(f"  • Scraping Operations: {config['scraping']['max_movies']}")
    print(f"  • Estimated Time: {estimated_time/60:.1f} minutes")
    
    # Confirm before starting
    print("\n⚠️  WARNING: This will make thousands of API calls!")
    confirm = input("Proceed with FULL pipeline? (type 'YES' to confirm): ").strip()
    if confirm != 'YES':
        print("Pipeline cancelled.")
        return False
    
    # Run the pipeline
    print("\n" + "=" * 70)
    print("🚀 STARTING FULL TOP-RATED MOVIES PIPELINE")
    print("=" * 70)
    
    start_time = time.time()
    
    try:
        from data_pipeline.top_rated_ingestor import TopRatedMoviesIngestor
        
        ingestor = TopRatedMoviesIngestor()
        success = ingestor.run_top_rated_pipeline(
            max_pages=config['tmdb']['max_pages'],
            max_omdb=config['omdb']['max_enrichments'],
            max_scraping=config['scraping']['max_movies']
        )
        
        if success:
            end_time = time.time()
            duration = end_time - start_time
            
            print("\n" + "=" * 70)
            print("🎉 FULL TOP-RATED MOVIES PIPELINE COMPLETED SUCCESSFULLY! 🎉")
            print("=" * 70)
            print(f"Total execution time: {duration/60:.2f} minutes ({duration:.0f} seconds)")
            print(f"Check the data/raw/ directory for staged data files")
            print("\n📁 Generated Files:")
            
            # List generated files
            data_dir = Path("data/raw")
            if data_dir.exists():
                for file in data_dir.glob("tmdb_top_rated_*"):
                    size_mb = file.stat().st_size / (1024 * 1024)
                    print(f"  • {file.name} ({size_mb:.1f} MB)")
            
            print("\n🔄 Next Steps:")
            print("1. Review the staged data files")
            print("2. Load data into the data lakehouse using stage_data_loader.py")
            print("3. Run analytical queries on the new data")
            print("4. Consider running scraping operations separately if needed")
            print("=" * 70)
            return True
        else:
            print("\n❌ Pipeline failed. Check the logs for details.")
            return False
            
    except Exception as e:
        print(f"\n💥 Unexpected error: {e}")
        return False

if __name__ == "__main__":
    try:
        success = main()
        if success:
            sys.exit(0)
        else:
            sys.exit(1)
    except KeyboardInterrupt:
        print("\n\n⏹️  Pipeline interrupted by user")
        sys.exit(1)
    except Exception as e:
        print(f"\n💥 Unexpected error: {e}")
        sys.exit(1)
