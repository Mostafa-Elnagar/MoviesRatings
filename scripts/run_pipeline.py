#!/usr/bin/env python3
"""
Movie Ratings Data Pipeline Runner
Main script to run the complete data pipeline
"""

import sys
import time
from pathlib import Path

# Add project root to path for imports
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from data_pipeline import (
    ComprehensiveIngestor, 
    config,
)

def run_comprehensive_ingestion(max_movies=None):
    """Run comprehensive ingestion with all data sources"""
    print("Starting Comprehensive Ingestion Pipeline")
    print("=" * 60)
    
    try:
        ingestor = ComprehensiveIngestor()
        results = ingestor.run_comprehensive_ingestion(max_movies)
        
        if results['success']:
            print(f"\nComprehensive ingestion completed successfully!")
            print(f"Files processed: {results['files_processed']}")
            print(f"Movies processed: {results['total_movies']}")
            print(f"Output files: {len(results['output_files'])}")
            
            # Print enhancement statistics
            stats = results['enhancement_stats']
            print(f"\nEnhancement Statistics:")
            print(f"   - OMDb enhanced: {stats['omdb_enhanced']}")
            print(f"   - Metacritic enhanced: {stats['metacritic_enhanced']}")
            print(f"   - Rotten Tomatoes enhanced: {stats['rotten_tomatoes_enhanced']}")
            print(f"   - Database inserted: {stats['database_inserted']}")
        else:
            print(f"\nComprehensive ingestion failed!")
            
        # Cleanup
        ingestor.cleanup()
        return results['success']
        
    except Exception as e:
        print(f"\nComprehensive ingestion error: {e}")
        return False

def main():
    """Main pipeline runner"""
    print("MOVIE RATINGS DATA PIPELINE")
    print("=" * 60)
    
    # Validate configuration
    if not config.validate_config():
        print("Configuration validation failed. Please check your setup.")
        sys.exit(1)
    
    # Print configuration summary
    config.print_config_summary()
    
    # Check command line arguments for max movies
    max_movies = None
    
    if len(sys.argv) > 1:
        try:
            max_movies = int(sys.argv[1])
        except ValueError:
            print(f"Invalid number: {sys.argv[1]}. Processing all movies.")
    
    if max_movies:
        print(f"Max movies per file: {max_movies}")
    else:
        print("Processing all available movies")
    
    # Run comprehensive ingestion
    start_time = time.time()
    success = run_comprehensive_ingestion(max_movies)
    
    # Calculate processing time
    elapsed_time = time.time() - start_time
    if elapsed_time < 60:
        time_str = f"{elapsed_time:.1f} seconds"
    elif elapsed_time < 3600:
        time_str = f"{elapsed_time/60:.1f} minutes"
    else:
        time_str = f"{elapsed_time/3600:.1f} hours"
    
    # Final results
    print("\n" + "=" * 60)
    if success:
        print("PIPELINE COMPLETED SUCCESSFULLY!")
        print(f"Total processing time: {time_str}")
    else:
        print("PIPELINE FAILED!")
        print(f"Processing time before failure: {time_str}")
    
    print("=" * 60)
    
    sys.exit(0 if success else 1)

if __name__ == "__main__":
    main()
