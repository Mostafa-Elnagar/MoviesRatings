#!/usr/bin/env python3
"""
Test script for the comprehensive ingestion pipeline
Tests individual components and basic functionality
"""

import sys
import json
from pathlib import Path

# Add project root to path for imports
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from data_pipeline.comprehensive_ingestor import ComprehensiveIngestor
from data_pipeline.scrappers.metacritic_scraper import MetacriticScraper
from data_pipeline.scrappers.tomatos_scraper import RottenTomatoesScraper

def test_ingestor_initialization():
    """Test that the ingestor can be initialized"""
    print("Testing ingestor initialization...")
    try:
        ingestor = ComprehensiveIngestor()
        print("✓ ComprehensiveIngestor initialized successfully")
        return True
    except Exception as e:
        print(f"✗ Failed to initialize ComprehensiveIngestor: {e}")
        return False

def test_scraper_initialization():
    """Test that scrapers can be initialized"""
    print("Testing scraper initialization...")
    
    try:
        metacritic_scraper = MetacriticScraper()
        print("✓ MetacriticScraper initialized successfully")
    except Exception as e:
        print(f"✗ Failed to initialize MetacriticScraper: {e}")
        return False
    
    try:
        rt_scraper = RottenTomatoesScraper(headless=True)
        print("✓ RottenTomatoesScraper initialized successfully")
        rt_scraper.close()  # Clean up
    except Exception as e:
        print(f"✗ Failed to initialize RottenTomatoesScraper: {e}")
        return False
    
    return True

def test_tmdb_file_detection():
    """Test that TMDB files can be detected"""
    print("Testing TMDB file detection...")
    
    try:
        ingestor = ComprehensiveIngestor()
        tmdb_files = ingestor.get_tmdb_raw_files()
        
        if tmdb_files:
            print(f"✓ Found {len(tmdb_files)} TMDB files:")
            for file in tmdb_files:
                print(f"  - {file.name}")
        else:
            print("⚠ No TMDB files found in raw directory")
        
        return True
    except Exception as e:
        print(f"✗ Failed to detect TMDB files: {e}")
        return False

def test_sample_data_loading():
    """Test loading sample TMDB data"""
    print("Testing sample data loading...")
    
    try:
        ingestor = ComprehensiveIngestor()
        tmdb_files = ingestor.get_tmdb_raw_files()
        
        if not tmdb_files:
            print("⚠ No TMDB files to test with")
            return True
        
        # Test with first file
        sample_file = tmdb_files[0]
        movies = ingestor.load_tmdb_data(sample_file)
        
        if movies:
            print(f"✓ Successfully loaded {len(movies)} movies from {sample_file.name}")
            
            # Show sample movie structure
            if len(movies) > 0:
                sample_movie = movies[0]
                print(f"  Sample movie: {sample_movie.get('title', 'Unknown')}")
                print(f"  Keys: {list(sample_movie.keys())[:10]}...")
        else:
            print("⚠ No movies loaded from sample file")
        
        return True
    except Exception as e:
        print(f"✗ Failed to load sample data: {e}")
        return False

def test_omdb_enhancement():
    """Test OMDb API enhancement (if API key available)"""
    print("Testing OMDb enhancement...")
    
    try:
        ingestor = ComprehensiveIngestor()
        
        if not ingestor.omdb_api_key:
            print("⚠ No OMDb API key found, skipping test")
            return True
        
        # Test with a known movie
        test_movie = {
            'title': 'The Shawshank Redemption',
            'year': 1994,
            'imdb_id': 'tt0111161'
        }
        
        enhanced_movie = ingestor.enhance_with_omdb(test_movie)
        
        if enhanced_movie.get('omdb_title'):
            print("✓ OMDb enhancement successful")
            print(f"  Enhanced title: {enhanced_movie['omdb_title']}")
            print(f"  Enhanced fields: {[k for k in enhanced_movie.keys() if k.startswith('omdb_')]}")
        else:
            print("⚠ OMDb enhancement returned no data")
        
        return True
    except Exception as e:
        print(f"✗ OMDb enhancement failed: {e}")
        return False

def test_metacritic_scraping():
    """Test Metacritic scraping with a known movie"""
    print("Testing Metacritic scraping...")
    
    try:
        scraper = MetacriticScraper()
        
        # Test with a known movie
        ratings = scraper.get_ratings('The Shawshank Redemption', 1994)
        
        if ratings and ratings.get('critic_score') is not None:
            print("✓ Metacritic scraping successful")
            print(f"  Critic score: {ratings.get('critic_score')}")
            print(f"  User score: {ratings.get('user_score')}")
        else:
            print("⚠ Metacritic scraping returned no data")
        
        return True
    except Exception as e:
        print(f"✗ Metacritic scraping failed: {e}")
        return False

def test_rotten_tomatoes_scraping():
    """Test Rotten Tomatoes scraping with a known movie"""
    print("Testing Rotten Tomatoes scraping...")
    
    try:
        scraper = RottenTomatoesScraper(headless=True)
        
        # Test with a known movie
        ratings = scraper.get_ratings('The Shawshank Redemption', 1994)
        
        if ratings and ratings.get('critic_score') is not None:
            print("✓ Rotten Tomatoes scraping successful")
            print(f"  Critic score: {ratings.get('critic_score')}")
            print(f"  User score: {ratings.get('user_score')}")
        else:
            print("⚠ Rotten Tomatoes scraping returned no data")
        
        scraper.close()  # Clean up
        return True
    except Exception as e:
        print(f"✗ Rotten Tomatoes scraping failed: {e}")
        return False

def run_all_tests():
    """Run all tests and report results"""
    print("=" * 60)
    print("COMPREHENSIVE INGESTION PIPELINE TEST SUITE")
    print("=" * 60)
    print()
    
    tests = [
        ("Ingestor Initialization", test_ingestor_initialization),
        ("Scraper Initialization", test_scraper_initialization),
        ("TMDB File Detection", test_tmdb_file_detection),
        ("Sample Data Loading", test_sample_data_loading),
        ("OMDb Enhancement", test_omdb_enhancement),
        ("Metacritic Scraping", test_metacritic_scraping),
        ("Rotten Tomatoes Scraping", test_rotten_tomatoes_scraping),
    ]
    
    passed = 0
    total = len(tests)
    
    for test_name, test_func in tests:
        print(f"\n{test_name}:")
        print("-" * 40)
        
        try:
            if test_func():
                passed += 1
        except Exception as e:
            print(f"✗ Test failed with exception: {e}")
    
    print("\n" + "=" * 60)
    print("TEST RESULTS SUMMARY")
    print("=" * 60)
    print(f"Tests passed: {passed}/{total}")
    
    if passed == total:
        print("🎉 All tests passed! The ingestion pipeline is ready to use.")
        return True
    else:
        print(f"⚠ {total - passed} test(s) failed. Please review the issues above.")
        return False

def main():
    """Main test runner"""
    success = run_all_tests()
    
    if success:
        print("\n✅ Test suite completed successfully")
        sys.exit(0)
    else:
        print("\n❌ Test suite completed with failures")
        sys.exit(1)

if __name__ == "__main__":
    main()
