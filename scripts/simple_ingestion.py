#!/usr/bin/env python3
"""
Simplified Movie Data Ingestion Pipeline
Enhances TMDB data with OMDb API information and inserts into Iceberg table
"""

import sys
import json
import os
import time
from pathlib import Path
from datetime import datetime
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Add project root to path for imports
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from data_pipeline.db_inserter import DatabaseInserter

class SimpleIngestor:
    """Simple ingestor that enhances TMDB data with OMDb API and inserts into database"""
    
    def __init__(self):
        self.omdb_api_key = os.getenv('OMDB_API_KEY')
        if not self.omdb_api_key:
            print("❌ OMDb API key not found in .env file!")
            print("Please ensure OMDB_API_KEY is set in your .env file")
            sys.exit(1)
        
        print(f"✅ OMDb API key loaded successfully")
        
        self.omdb_base_url = "http://www.omdbapi.com/"
        self.raw_data_dir = Path("data/raw")
        self.processed_data_dir = Path("data/processed")
        
        # Ensure directories exist
        self.processed_data_dir.mkdir(parents=True, exist_ok=True)
        
        # Rate limiting
        self.omdb_delay = 0.1  # 100ms between API calls
        
        # Database inserter
        self.db_inserter = DatabaseInserter()
        
        # Statistics
        self.stats = {
            'total_movies': 0,
            'omdb_enhanced': 0,
            'errors': 0,
            'db_inserted': 0
        }
    
    def get_tmdb_files(self):
        """Get all TMDB raw data files"""
        tmdb_files = list(self.raw_data_dir.glob("tmdb_*.json"))
        if not tmdb_files:
            print("❌ No TMDB files found in data/raw/")
            return []
        
        print(f"📁 Found {len(tmdb_files)} TMDB files:")
        for file in tmdb_files:
            print(f"   - {file.name}")
        return tmdb_files
    
    def load_tmdb_data(self, file_path):
        """Load TMDB data from JSON file"""
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
            print(f"📊 Loaded {len(data)} movies from {file_path.name}")
            return data
        except Exception as e:
            print(f"❌ Error loading {file_path}: {e}")
            return []
    
    def enhance_movie(self, movie):
        """Enhance a single movie with OMDb data"""
        # Get IMDb ID
        imdb_id = movie.get('imdb_id')
        if not imdb_id:
            return movie
        
        try:
            # Call OMDb API
            params = {
                'apikey': self.omdb_api_key,
                'i': imdb_id,
                'plot': 'full'
            }
            
            import requests
            response = requests.get(self.omdb_base_url, params=params, timeout=10)
            response.raise_for_status()
            
            omdb_data = response.json()
            
            if omdb_data.get('Response') == 'True':
                # Enhance movie data
                enhanced_movie = movie.copy()
                enhanced_movie.update({
                    'omdb_title': omdb_data.get('Title'),
                    'omdb_rated': omdb_data.get('Rated'),
                    'omdb_released': omdb_data.get('Released'),
                    'omdb_runtime': omdb_data.get('Runtime'),
                    'omdb_genre': omdb_data.get('Genre'),
                    'omdb_director': omdb_data.get('Director'),
                    'omdb_writer': omdb_data.get('Writer'),
                    'omdb_actors': omdb_data.get('Actors'),
                    'omdb_plot': omdb_data.get('Plot'),
                    'omdb_language': omdb_data.get('Language'),
                    'omdb_country': omdb_data.get('Country'),
                    'omdb_awards': omdb_data.get('Awards'),
                    'omdb_poster': omdb_data.get('Poster'),
                    'omdb_ratings': omdb_data.get('Ratings', []),
                    'omdb_imdb_rating': self._parse_rating(omdb_data.get('imdbRating')),
                    'omdb_imdb_votes': self._parse_votes(omdb_data.get('imdbVotes')),
                    'omdb_metascore': self._parse_metascore(omdb_data.get('Metascore')),
                    'omdb_box_office': omdb_data.get('BoxOffice'),
                    'omdb_production': omdb_data.get('Production'),
                    'omdb_website': omdb_data.get('Website'),
                    'omdb_enhanced_at': datetime.now().isoformat()
                })
                
                self.stats['omdb_enhanced'] += 1
                return enhanced_movie
            else:
                return movie
                
        except Exception as e:
            print(f"⚠️ Error enhancing {movie.get('title', 'Unknown')}: {e}")
            self.stats['errors'] += 1
            return movie
        
        finally:
            time.sleep(self.omdb_delay)
    
    def _parse_rating(self, rating_str):
        """Parse rating string to float"""
        if not rating_str or rating_str == 'N/A':
            return None
        try:
            return float(rating_str)
        except ValueError:
            return None
    
    def _parse_votes(self, votes_str):
        """Parse votes string to integer"""
        if not votes_str or votes_str == 'N/A':
            return None
        try:
            return int(votes_str.replace(',', ''))
        except ValueError:
            return None
    
    def _parse_metascore(self, metascore_str):
        """Parse metascore string to integer"""
        if not metascore_str or metascore_str == 'N/A':
            return None
        try:
            return int(metascore_str)
        except ValueError:
            return None
    
    def process_file(self, file_path, max_movies=None):
        """Process a single TMDB file"""
        print(f"\n🎬 Processing: {file_path.name}")
        
        # Load TMDB data
        movies = self.load_tmdb_data(file_path)
        if not movies:
            return False
        
        # Limit movies if specified
        if max_movies:
            movies = movies[:max_movies]
            print(f"📝 Processing limited to {max_movies} movies")
        
        # Process each movie
        enhanced_movies = []
        total_movies = len(movies)
        
        for i, movie in enumerate(movies):
            print(f"   🎥 {i+1:3d}/{total_movies}: {movie.get('title', 'Unknown')}")
            
            # Add basic metadata
            movie['data_source'] = 'tmdb'
            movie['processed_at'] = datetime.now().isoformat()
            
            # Enhance with OMDb
            enhanced_movie = self.enhance_movie(movie)
            enhanced_movies.append(enhanced_movie)
            
            # Progress update
            if (i + 1) % 10 == 0:
                print(f"      ✅ Processed {i+1}/{total_movies} movies")
        
        # Save enhanced data
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_filename = f"enhanced_{file_path.stem}_{timestamp}.json"
        output_path = self.processed_data_dir / output_filename
        
        try:
            with open(output_path, 'w', encoding='utf-8') as f:
                json.dump(enhanced_movies, f, indent=2, ensure_ascii=False)
            
            print(f"💾 Saved {len(enhanced_movies)} enhanced movies to {output_filename}")
            self.stats['total_movies'] += len(enhanced_movies)
            
            # Insert into database
            print(f"🗄️ Inserting {len(enhanced_movies)} movies into database...")
            if self.insert_to_database(enhanced_movies):
                self.stats['db_inserted'] += len(enhanced_movies)
                print(f"✅ Successfully inserted {len(enhanced_movies)} movies into database")
            else:
                print(f"❌ Failed to insert movies into database")
            
            return True
            
        except Exception as e:
            print(f"❌ Error saving data: {e}")
            return False
    
    def insert_to_database(self, enhanced_movies):
        """Insert enhanced movies into the Iceberg table"""
        try:
            # Test database connection first
            if not self.db_inserter.test_connection():
                print("❌ Database connection test failed")
                return False
            
            # Get table info to verify schema
            table_info = self.db_inserter.get_table_info()
            if not table_info:
                print("❌ Failed to get table information")
                return False
            
            print(f"📋 Target table: {table_info['table_name']} ({table_info['column_count']} columns)")
            
            # Perform bulk insert
            success = self.db_inserter.bulk_insert(enhanced_movies, batch_size=50)
            
            # Clean up connection
            self.db_inserter.disconnect()
            
            return success
            
        except Exception as e:
            print(f"❌ Database insertion error: {e}")
            return False
    
    def run_ingestion(self, max_movies=None):
        """Run the complete ingestion pipeline"""
        print("🚀 Starting Movie Data Ingestion Pipeline")
        print("=" * 60)
        
        # Get TMDB files
        tmdb_files = self.get_tmdb_files()
        if not tmdb_files:
            return False
        
        # Process each file
        success_count = 0
        for file_path in tmdb_files:
            if self.process_file(file_path, max_movies):
                success_count += 1
        
        # Print statistics
        self._print_statistics()
        
        if success_count > 0:
            print(f"\n🎉 Successfully processed {success_count}/{len(tmdb_files)} files!")
            return True
        else:
            print("\n❌ No files were processed successfully")
            return False
    
    def _print_statistics(self):
        """Print ingestion statistics"""
        print("\n" + "=" * 50)
        print("📊 INGESTION STATISTICS")
        print("=" * 50)
        print(f"Total movies processed: {self.stats['total_movies']}")
        print(f"OMDb enhanced: {self.stats['omdb_enhanced']}")
        print(f"Database inserted: {self.stats['db_inserted']}")
        print(f"Errors encountered: {self.stats['errors']}")
        print("=" * 50)

def main():
    """Main function"""
    print("🎬 MOVIE RATINGS DATA INGESTION PIPELINE")
    print("=" * 60)
    
    # Initialize ingestor
    ingestor = SimpleIngestor()
    
    # Check command line arguments
    max_movies = None
    if len(sys.argv) > 1:
        try:
            max_movies = int(sys.argv[1])
            print(f"📝 Limiting processing to {max_movies} movies per file")
        except ValueError:
            print(f"⚠️ Invalid number: {sys.argv[1]}. Processing all movies.")
    
    # Run ingestion
    success = ingestor.run_ingestion(max_movies)
    
    if success:
        print("\n✅ INGESTION PIPELINE COMPLETED SUCCESSFULLY!")
        print("\nEnhanced data saved to: data/processed/")
        print("Data inserted into: iceberg.movies_stage.omdb_movies")
        sys.exit(0)
    else:
        print("\n❌ INGESTION PIPELINE FAILED!")
        sys.exit(1)

if __name__ == "__main__":
    main()
