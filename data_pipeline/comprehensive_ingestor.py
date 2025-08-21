#!/usr/bin/env python3
"""
Comprehensive Movie Data Ingestion Pipeline
Integrates TMDB, OMDb API, Metacritic, and Rotten Tomatoes data sources
"""

import json
import os
import time
import logging
from pathlib import Path
from datetime import datetime
from typing import List, Dict, Any, Optional

from .ingestor import SimpleIngestor
from .multi_table_inserter import MultiTableInserter

logger = logging.getLogger(__name__)

class ComprehensiveIngestor:
    """Comprehensive ingestor that integrates all movie data sources"""
    
    def __init__(self, omdb_api_key: Optional[str] = None, headless: bool = True):
        self.omdb_api_key = omdb_api_key or os.getenv('OMDB_API_KEY')
        if not self.omdb_api_key:
            raise ValueError("OMDb API key is required")
        
        # Initialize components
        self.base_ingestor = SimpleIngestor(self.omdb_api_key)
        self.multi_inserter = MultiTableInserter()
        
        # Initialize scrapers with error handling
        self.scrapers_available = False
        try:
            from .scrappers.metacritic_scraper import MetacriticScraper
            from .scrappers.tomatos_scraper import RottenTomatoesScraper
            
            self.metacritic_scraper = MetacriticScraper()
            self.rotten_tomatoes_scraper = RottenTomatoesScraper(headless=headless)
            self.scrapers_available = True
            logger.info("Scrapers initialized successfully")
        except Exception as e:
            logger.warning(f"Scrapers not available: {e}")
            logger.info("Continuing with OMDb API only")
        
        # Statistics
        self.stats = {
            'total_movies': 0,
            'omdb_enhanced': 0,
            'metacritic_enhanced': 0,
            'rotten_tomatoes_enhanced': 0,
            'database_inserted': 0,
            'errors': 0
        }
    
    def enhance_with_metacritic(self, movie: Dict[str, Any]) -> Dict[str, Any]:
        """Enhance movie with Metacritic data"""
        if not self.scrapers_available:
            return movie
            
        try:
            title = movie.get('title') or movie.get('omdb_title')
            year = movie.get('year')
            
            # Extract year from release_date if needed
            if not year and movie.get('release_date'):
                try:
                    year = int(movie['release_date'][:4])
                except (ValueError, TypeError):
                    pass
            
            if not title or not year:
                return movie
            
            # Get ratings from Metacritic
            metacritic_data = self.metacritic_scraper.get_ratings(title, year)
            
            if metacritic_data and metacritic_data.get('critic_score') is not None:
                enhanced_movie = movie.copy()
                enhanced_movie.update({
                    'metacritic_critic_score': metacritic_data.get('critic_score'),
                    'metacritic_critic_count': metacritic_data.get('critic_count'),
                    'metacritic_user_score': metacritic_data.get('user_score'),
                    'metacritic_user_count': metacritic_data.get('user_count'),
                    'metacritic_data_source': 'metacritic',
                    'metacritic_enhanced_at': datetime.now().isoformat()
                })
                
                self.stats['metacritic_enhanced'] += 1
                return enhanced_movie
            
            return movie
            
        except Exception as e:
            self.stats['errors'] += 1
            logger.warning(f"Metacritic enhancement failed for {movie.get('title', 'Unknown')}: {e}")
            return movie
        
        finally:
            time.sleep(1.0)  # Rate limiting
    
    def enhance_with_rotten_tomatoes(self, movie: Dict[str, Any]) -> Dict[str, Any]:
        """Enhance movie with Rotten Tomatoes data"""
        if not self.scrapers_available:
            return movie
            
        try:
            title = movie.get('title') or movie.get('omdb_title')
            year = movie.get('year')
            
            # Extract year from release_date if needed
            if not year and movie.get('release_date'):
                try:
                    year = int(movie['release_date'][:4])
                except (ValueError, TypeError):
                    pass
            
            if not title or not year:
                return movie
            
            # Get ratings from Rotten Tomatoes
            rt_data = self.rotten_tomatoes_scraper.get_ratings(title, year)
            
            if rt_data and rt_data.get('critic_score') is not None:
                enhanced_movie = movie.copy()
                enhanced_movie.update({
                    'rt_critic_score': rt_data.get('critic_score'),
                    'rt_user_score': rt_data.get('user_score'),
                    'rt_user_count': rt_data.get('user_count'),
                    'rt_data_source': 'rottentomatoes',
                    'rt_enhanced_at': datetime.now().isoformat()
                })
                
                self.stats['rotten_tomatoes_enhanced'] += 1
                return enhanced_movie
            
            return movie
            
        except Exception as e:
            self.stats['errors'] += 1
            logger.warning(f"Rotten Tomatoes enhancement failed for {movie.get('title', 'Unknown')}: {e}")
            return movie
        
        finally:
            time.sleep(1.0)  # Rate limiting
    
    def process_movie_comprehensive(self, movie: Dict[str, Any]) -> Dict[str, Any]:
        """Process a single movie through all enhancement stages"""
        try:
            # Stage 1: OMDb enhancement (always available)
            enhanced_movie = self.base_ingestor.enhance_movie(movie)
            if enhanced_movie.get('omdb_title'):
                self.stats['omdb_enhanced'] += 1
            
            # Stage 2: Metacritic enhancement (if available)
            enhanced_movie = self.enhance_with_metacritic(enhanced_movie)
            
            # Stage 3: Rotten Tomatoes enhancement (if available)
            enhanced_movie = self.enhance_with_rotten_tomatoes(enhanced_movie)
            
            return enhanced_movie
            
        except Exception as e:
            self.stats['errors'] += 1
            logger.error(f"Comprehensive enhancement failed for {movie.get('title', 'Unknown')}: {e}")
            # Return the movie as-is if enhancement fails
            return movie
    
    def process_file_comprehensive(self, file_path: Path, max_movies: Optional[int] = None) -> List[Dict[str, Any]]:
        """Process a single TMDB file with comprehensive enhancement"""
        movies = self.base_ingestor.load_tmdb_data(file_path)
        
        if max_movies:
            movies = movies[:max_movies]
        
        enhanced_movies = []
        total_movies = len(movies)
        
        print(f"   Processing {total_movies} movies...")
        
        for i, movie in enumerate(movies):
            try:
                print(f"   Movie {i+1:3d}/{total_movies}: {movie.get('title', 'Unknown')}")
                
                # Add basic metadata
                movie['data_source'] = 'tmdb'
                movie['processed_at'] = datetime.now().isoformat()
                
                # Comprehensive enhancement
                enhanced_movie = self.process_movie_comprehensive(movie)
                enhanced_movies.append(enhanced_movie)
                
                # Progress update
                if (i + 1) % 10 == 0:
                    print(f"      Processed {i+1}/{total_movies} movies")
                
            except Exception as e:
                logger.error(f"Error processing movie {i+1}: {e}")
                # Add the original movie to continue processing
                enhanced_movies.append(movie)
                self.stats['errors'] += 1
        
        self.stats['total_movies'] += len(enhanced_movies)
        return enhanced_movies
    
    def save_comprehensive_data(self, enhanced_movies: List[Dict[str, Any]], 
                               output_filename: str) -> Path:
        """Save comprehensively enhanced data"""
        return self.base_ingestor.save_enhanced_data(enhanced_movies, output_filename)
    
    def insert_to_database(self, enhanced_movies: List[Dict[str, Any]]) -> bool:
        """Insert enhanced movies into appropriate tables"""
        try:
            if not self.multi_inserter.test_connection():
                logger.error("Database connection test failed")
                return False
            
            # Insert into all appropriate tables
            results = self.multi_inserter.insert_all_data(enhanced_movies)
            
            # Check results
            all_success = all(results.values())
            if all_success:
                self.stats['database_inserted'] += len(enhanced_movies)
                logger.info("Successfully inserted data into all tables")
            else:
                failed_tables = [table for table, success in results.items() if not success]
                logger.warning(f"Failed to insert into tables: {failed_tables}")
            
            self.multi_inserter.disconnect()
            return all_success
            
        except Exception as e:
            logger.error(f"Database insertion error: {e}")
            return False
    
    def run_comprehensive_ingestion(self, max_movies: Optional[int] = None) -> Dict[str, Any]:
        """Run the complete comprehensive ingestion pipeline"""
        tmdb_files = self.base_ingestor.get_tmdb_files()
        
        if not tmdb_files:
            logger.error("No TMDB files found")
            return {'success': False, 'error': 'No TMDB files found'}
        
        results = {
            'files_processed': 0,
            'total_movies': 0,
            'enhanced_movies': [],
            'output_files': [],
            'success': True,
            'enhancement_stats': {}
        }
        
        for file_path in tmdb_files:
            try:
                print(f"\nProcessing: {file_path.name}")
                
                # Process the file
                enhanced_movies = self.process_file_comprehensive(file_path, max_movies)
                
                # Save enhanced data
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                output_filename = f"comprehensive_enhanced_{file_path.stem}_{timestamp}.json"
                output_path = self.save_comprehensive_data(enhanced_movies, output_filename)
                
                # Insert into database
                db_success = self.insert_to_database(enhanced_movies)
                
                results['files_processed'] += 1
                results['total_movies'] += len(enhanced_movies)
                results['enhanced_movies'].extend(enhanced_movies)
                results['output_files'].append(str(output_path))
                
                if not db_success:
                    results['success'] = False
                
                print(f"Saved {len(enhanced_movies)} enhanced movies to {output_filename}")
                if db_success:
                    print(f"Successfully inserted {len(enhanced_movies)} movies into database")
                else:
                    print(f"Failed to insert movies into database")
                
                # Print current stats
                print(f"Current stats: OMDb: {self.stats['omdb_enhanced']}, "
                      f"Metacritic: {self.stats['metacritic_enhanced']}, "
                      f"RT: {self.stats['rotten_tomatoes_enhanced']}, "
                      f"Errors: {self.stats['errors']}")
                
            except Exception as e:
                results['success'] = False
                logger.error(f"Error processing {file_path}: {e}")
                print(f"Error processing {file_path.name}: {e}")
                continue  # Continue with next file instead of stopping
        
        results['enhancement_stats'] = self.get_statistics()
        return results
    
    def get_statistics(self) -> Dict[str, Any]:
        """Get comprehensive ingestion statistics"""
        return self.stats.copy()
    
    def reset_statistics(self):
        """Reset all statistics to zero"""
        self.stats = {
            'total_movies': 0,
            'omdb_enhanced': 0,
            'metacritic_enhanced': 0,
            'rotten_tomatoes_enhanced': 0,
            'database_inserted': 0,
            'errors': 0
        }
    
    def cleanup(self):
        """Clean up resources"""
        try:
            if hasattr(self, 'rotten_tomatoes_scraper') and self.scrapers_available:
                if hasattr(self.rotten_tomatoes_scraper, 'close'):
                    self.rotten_tomatoes_scraper.close()
                elif hasattr(self.rotten_tomatoes_scraper, 'cleanup'):
                    self.rotten_tomatoes_scraper.cleanup()
        except Exception as e:
            logger.warning(f"Cleanup warning: {e}")
