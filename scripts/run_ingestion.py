#!/usr/bin/env python3
"""
Runner script for the comprehensive movie data ingestion pipeline
"""

import sys
from pathlib import Path

# Add project root to path for imports
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from data_pipeline.comprehensive_ingestor import ComprehensiveIngestor

def main():
    """Run the comprehensive ingestion pipeline"""
    print("=" * 60)
    print("MOVIE RATINGS COMPREHENSIVE INGESTION PIPELINE")
    print("=" * 60)
    print()
    
    # Initialize ingestor
    ingestor = ComprehensiveIngestor()
    
    # Check if specific file is provided
    if len(sys.argv) > 1:
        input_file = Path(sys.argv[1])
        if input_file.exists():
            print(f"Processing specific file: {input_file}")
            success = ingestor.run_ingestion(input_file)
        else:
            print(f"Error: Input file not found: {input_file}")
            sys.exit(1)
    else:
        print("Processing all TMDB raw data files...")
        success = ingestor.run_ingestion()
    
    if success:
        print("\n" + "=" * 60)
        print("INGESTION PIPELINE COMPLETED SUCCESSFULLY!")
        print("=" * 60)
        sys.exit(0)
    else:
        print("\n" + "=" * 60)
        print("INGESTION PIPELINE FAILED!")
        print("=" * 60)
        sys.exit(1)

if __name__ == "__main__":
    main()
