#!/usr/bin/env python3
"""
Main Movie Data Ingestion Pipeline
Enhances TMDB data with OMDb API information and inserts into Iceberg table
"""

import json
import os
import time
from pathlib import Path
from datetime import datetime
from typing import List, Dict, Any, Optional

from .db_inserter import DatabaseInserter

class SimpleIngestor:
    """Simple ingestor that enhances TMDB data with OMDb API and inserts into database"""
    
    def __init__(self, omdb_api_key: Optional[str] = None):
        self.omdb_api_key = omdb_api_key or os.getenv('OMDB_API_KEY')
        if not self.omdb_api_key:
            raise ValueError("OMDb API key is required. Set OMDB_API_KEY environment variable or pass it to constructor.")
        
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
    
    def get_tmdb_files(self) -> List[Path]:
        """Get all TMDB raw data files"""
        tmdb_files = list(self.raw_data_dir.glob("tmdb_*.json"))
        if not tmdb_files:
            raise FileNotFoundError("No TMDB files found in data/raw/")
        return tmdb_files
    
    def load_tmdb_data(self, file_path: Path) -> List[Dict[str, Any]]:
        """Load TMDB data from JSON file"""
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
            return data
        except Exception as e:
            raise RuntimeError(f"Error loading {file_path}: {e}")
    
    def enhance_movie(self, movie: Dict[str, Any]) -> Dict[str, Any]:
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
            self.stats['errors'] += 1
            raise RuntimeError(f"Error enhancing {movie.get('title', 'Unknown')}: {e}")
        
        finally:
            time.sleep(self.omdb_delay)
    
    def _parse_rating(self, rating_str: Optional[str]) -> Optional[float]:
        """Parse rating string to float"""
        if not rating_str or rating_str == 'N/A':
            return None
        try:
            return float(rating_str)
        except ValueError:
            return None
    
    def _parse_votes(self, votes_str: Optional[str]) -> Optional[int]:
        """Parse votes string to integer"""
        if not votes_str or votes_str == 'N/A':
            return None
        try:
            return int(votes_str.replace(',', ''))
        except ValueError:
            return None
    
    def _parse_metascore(self, metascore_str: Optional[str]) -> Optional[int]:
        """Parse metascore string to integer"""
        if not metascore_str or metascore_str == 'N/A':
            return None
        try:
            return int(metascore_str)
        except ValueError:
            return None
    
    def process_file(self, file_path: Path, max_movies: Optional[int] = None) -> List[Dict[str, Any]]:
        """Process a single TMDB file and return enhanced movies"""
        # Load TMDB data
        movies = self.load_tmdb_data(file_path)
        
        # Limit movies if specified
        if max_movies:
            movies = movies[:max_movies]
        
        # Process each movie
        enhanced_movies = []
        total_movies = len(movies)
        
        for i, movie in enumerate(movies):
            # Add basic metadata
            movie['data_source'] = 'tmdb'
            movie['processed_at'] = datetime.now().isoformat()
            
            # Enhance with OMDb
            enhanced_movie = self.enhance_movie(movie)
            enhanced_movies.append(enhanced_movie)
        
        self.stats['total_movies'] += len(enhanced_movies)
        return enhanced_movies
    
    def save_enhanced_data(self, enhanced_movies: List[Dict[str, Any]], 
                          output_filename: str) -> Path:
        """Save enhanced data to processed directory"""
        output_path = self.processed_data_dir / output_filename
        
        try:
            with open(output_path, 'w', encoding='utf-8') as f:
                json.dump(enhanced_movies, f, indent=2, ensure_ascii=False)
            
            return output_path
            
        except Exception as e:
            raise RuntimeError(f"Error saving enhanced data to {output_path}: {e}")
    
    def insert_to_database(self, enhanced_movies: List[Dict[str, Any]]) -> bool:
        """Insert enhanced movies into the Iceberg table"""
        try:
            # Test database connection first
            if not self.db_inserter.test_connection():
                return False
            
            # Get table info to verify schema
            table_info = self.db_inserter.get_table_info()
            if not table_info:
                return False
            
            # Perform bulk insert
            success = self.db_inserter.bulk_insert(enhanced_movies, batch_size=50)
            
            if success:
                self.stats['db_inserted'] += len(enhanced_movies)
            
            # Clean up connection
            self.db_inserter.disconnect()
            
            return success
            
        except Exception as e:
            raise RuntimeError(f"Database insertion error: {e}")
    
    def run_ingestion(self, max_movies: Optional[int] = None) -> Dict[str, Any]:
        """Run the complete ingestion pipeline and return results"""
        # Get TMDB files
        tmdb_files = self.get_tmdb_files()
        
        results = {
            'files_processed': 0,
            'total_movies': 0,
            'enhanced_movies': [],
            'output_files': [],
            'success': True
        }
        
        # Process each file
        for file_path in tmdb_files:
            try:
                # Process the file
                enhanced_movies = self.process_file(file_path, max_movies)
                
                # Save enhanced data
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                output_filename = f"enhanced_{file_path.stem}_{timestamp}.json"
                output_path = self.save_enhanced_data(enhanced_movies, output_filename)
                
                # Insert into database
                db_success = self.insert_to_database(enhanced_movies)
                
                results['files_processed'] += 1
                results['total_movies'] += len(enhanced_movies)
                results['enhanced_movies'].extend(enhanced_movies)
                results['output_files'].append(str(output_path))
                
                if not db_success:
                    results['success'] = False
                
            except Exception as e:
                results['success'] = False
                raise RuntimeError(f"Error processing {file_path}: {e}")
        
        return results
    
    def get_statistics(self) -> Dict[str, Any]:
        """Get current ingestion statistics"""
        return self.stats.copy()
    
    def reset_statistics(self):
        """Reset all statistics to zero"""
        self.stats = {
            'total_movies': 0,
            'omdb_enhanced': 0,
            'errors': 0,
            'db_inserted': 0
        }
