#!/usr/bin/env python3
"""
Top-Rated Movies Ingestion Pipeline
Fetches all top-rated movies from TMDB's top-rated endpoint
"""

import os
import json
import logging
import time
from datetime import datetime
from typing import List, Dict, Any, Optional
from pathlib import Path
import requests
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("logs/top_rated_ingestion.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class TopRatedMoviesIngestor:
    """Ingest top-rated movies from TMDB"""
    
    def __init__(self):
        self.tmdb_api_key = os.getenv("TMDB_API_KEY")
        self.omdb_api_key = os.getenv("OMDB_API_KEY")
        
        if not self.tmdb_api_key:
            raise ValueError("TMDB_API_KEY environment variable is required")
        if not self.omdb_api_key:
            raise ValueError("OMDB_API_KEY environment variable is required")
        
        self.tmdb_base_url = "https://api.themoviedb.org/3"
        self.omdb_base_url = "https://www.omdbapi.com/"
        
        # Import scrapers
        from scrappers.metacritic_scraper import MetacriticScraper
        from scrappers.tomatos_scraper import RottenTomatoesScraper
        
        self.metacritic_scraper = MetacriticScraper()
        self.rotten_tomatoes_scraper = RottenTomatoesScraper()
        
        # Data storage
        self.staged_data = {
            'tmdb_movies': [],
            'omdb_movies': [],
            'metacritic_ratings': [],
            'rotten_tomatoes_ratings': []
        }
        
        # Create data directories
        os.makedirs("data/raw", exist_ok=True)
        os.makedirs("data/processed", exist_ok=True)
        os.makedirs("logs", exist_ok=True)
    
    def get_top_rated_movies(self, max_pages: int = 500, movies_per_page: int = 20) -> List[Dict[str, Any]]:
        """Get top-rated movies from TMDB, page by page"""
        logger.info(f"Fetching top-rated movies (max pages: {max_pages}, movies per page: {movies_per_page})")
        
        all_movies = []
        page = 1
        
        while page <= max_pages:
            try:
                logger.info(f"Fetching page {page}...")
                
                # Get top-rated movies for current page
                url = f"{self.tmdb_base_url}/movie/top_rated"
                params = {
                    'api_key': self.tmdb_api_key,
                    'language': 'en-US',
                    'page': page
                }
                
                response = requests.get(url, params=params, timeout=10)
                response.raise_for_status()
                data = response.json()
                
                movies = data.get('results', [])
                total_pages = data.get('total_pages', 0)
                total_results = data.get('total_results', 0)
                
                logger.info(f"Page {page}: Found {len(movies)} movies (Total: {total_results}, Pages: {total_pages})")
                
                if not movies:
                    logger.info(f"No more movies found on page {page}. Stopping.")
                    break
                
                # Process each movie on this page
                for movie in movies:
                    try:
                        # Get detailed movie info
                        movie_id = movie['id']
                        detailed_movie = self._get_movie_details(movie_id)
                        if detailed_movie:
                            all_movies.append(detailed_movie)
                    except Exception as e:
                        logger.error(f"Error processing movie {movie.get('id', 'unknown')}: {e}")
                        continue
                
                # Check if we've reached the last page
                if page >= total_pages:
                    logger.info(f"Reached last page ({total_pages}). Stopping.")
                    break
                
                page += 1
                
                # Rate limiting - TMDB allows 40 requests per 10 seconds
                time.sleep(0.25)  # 4 requests per second
                
            except Exception as e:
                logger.error(f"Error fetching page {page}: {e}")
                break
        
        logger.info(f"Successfully fetched {len(all_movies)} top-rated movies")
        self.staged_data['tmdb_movies'] = all_movies
        return all_movies
    
    def _get_movie_details(self, movie_id: int) -> Optional[Dict[str, Any]]:
        """Get detailed movie information including credits and external IDs"""
        try:
            url = f"{self.tmdb_base_url}/movie/{movie_id}"
            params = {
                'api_key': self.tmdb_api_key,
                'language': 'en-US',
                'append_to_response': 'credits,external_ids'
            }
            
            response = requests.get(url, params=params, timeout=10)
            response.raise_for_status()
            movie = response.json()
            
            # Get genre mapping
            genres_url = f"{self.tmdb_base_url}/genre/movie/list"
            genres_params = {'api_key': self.tmdb_api_key, 'language': 'en-US'}
            genres_response = requests.get(genres_url, params=genres_params, timeout=10)
            genres_response.raise_for_status()
            genres_data = genres_response.json()
            genre_map = {g['id']: g['name'] for g in genres_data.get('genres', [])}
            
            # Process movie data
            processed_movie = self._process_tmdb_movie(movie, genre_map)
            return processed_movie
            
        except Exception as e:
            logger.error(f"Error getting details for movie {movie_id}: {e}")
            return None
    
    def _process_tmdb_movie(self, movie: Dict[str, Any], genre_map: Dict[int, str]) -> Dict[str, Any]:
        """Process TMDB movie data into standardized format"""
        # Extract genres
        genres = [genre_map.get(g['id'], str(g['id'])) for g in movie.get('genres', [])]
        genre_ids = [g['id'] for g in movie.get('genres', [])]
        
        # Extract cast and crew (top 10)
        credits = movie.get('credits', {})
        cast = [{'id': p['id'], 'name': p['name'], 'character': p.get('character', '')} 
                for p in credits.get('cast', [])[:10]]
        crew = [{'id': p['id'], 'name': p['name'], 'job': p.get('job', '')} 
                for p in credits.get('crew', [])[:10]]
        
        # Extract external IDs
        external_ids = movie.get('external_ids', {})
        imdb_id = external_ids.get('imdb_id')
        
        return {
            'tmdb_id': movie.get('id'),
            'imdb_id': imdb_id,
            'title': movie.get('title'),
            'original_title': movie.get('original_title'),
            'release_date': movie.get('release_date'),
            'year': int(movie.get('release_date', '0000')[:4]) if movie.get('release_date') else None,
            'overview': movie.get('overview'),
            'tagline': movie.get('tagline'),
            'status': movie.get('status'),
            'runtime': movie.get('runtime'),
            'budget': movie.get('budget'),
            'revenue': movie.get('revenue'),
            'popularity': movie.get('popularity'),
            'vote_average': movie.get('vote_average'),
            'vote_count': movie.get('vote_count'),
            'genres': genres,
            'genre_ids': genre_ids,
            'original_language': movie.get('original_language'),
            'production_companies': [c['name'] for c in movie.get('production_companies', [])],
            'production_countries': [c['name'] for c in movie.get('production_countries', [])],
            'spoken_languages': [l['name'] for l in movie.get('spoken_languages', [])],
            'cast': cast,
            'crew': crew,
            'backdrop_path': movie.get('backdrop_path'),
            'poster_path': movie.get('poster_path'),
            'homepage': movie.get('homepage'),
            'external_ids': external_ids,
            'data_source': 'tmdb_top_rated',
            'created_at': datetime.now().isoformat(),
            'updated_at': datetime.now().isoformat()
        }
    
    def get_omdb_data_for_movies(self, movies: List[Dict[str, Any]], max_movies: int = 100) -> List[Dict[str, Any]]:
        """Get OMDb data for movies that have IMDB IDs"""
        logger.info(f"Fetching OMDb data for {len(movies)} movies (max: {max_movies})")
        
        omdb_movies = []
        movies_with_imdb = [m for m in movies if m.get('imdb_id')]
        
        for i, movie in enumerate(movies_with_imdb[:max_movies]):
            try:
                imdb_id = movie['imdb_id']
                logger.info(f"Processing OMDb data {i+1}/{len(movies_with_imdb[:max_movies])}: {imdb_id}")
                
                url = self.omdb_base_url
                params = {
                    'apikey': self.omdb_api_key,
                    'i': imdb_id,
                    'plot': 'full'
                }
                
                response = requests.get(url, params=params, timeout=10)
                response.raise_for_status()
                omdb_data = response.json()
                
                if omdb_data.get('Response') == 'True':
                    processed_omdb = self._process_omdb_movie(omdb_data, movie)
                    omdb_movies.append(processed_omdb)
                    logger.info(f"Successfully processed OMDb data for {omdb_data.get('Title', 'Unknown')}")
                else:
                    logger.warning(f"No OMDb data found for IMDB ID: {imdb_id}")
                
                # Rate limiting for OMDb API
                time.sleep(0.1)  # 10 requests per second
                
            except Exception as e:
                logger.error(f"Error processing OMDb data for {movie.get('imdb_id', 'unknown')}: {e}")
                continue
        
        logger.info(f"Successfully processed {len(omdb_movies)} OMDb movies")
        self.staged_data['omdb_movies'] = omdb_movies
        return omdb_movies
    
    def _process_omdb_movie(self, omdb_data: Dict[str, Any], tmdb_movie: Dict[str, Any]) -> Dict[str, Any]:
        """Process OMDb movie data into standardized format"""
        return {
            'imdb_id': omdb_data.get('imdbID'),
            'title': tmdb_movie.get('title'),
            'year': tmdb_movie.get('year'),
            'omdb_title': omdb_data.get('Title'),
            'rated': omdb_data.get('Rated'),
            'released': omdb_data.get('Released'),
            'runtime': omdb_data.get('Runtime'),
            'genre': omdb_data.get('Genre'),
            'director': omdb_data.get('Director'),
            'writer': omdb_data.get('Writer'),
            'actors': omdb_data.get('Actors'),
            'plot': omdb_data.get('Plot'),
            'language': omdb_data.get('Language'),
            'country': omdb_data.get('Country'),
            'awards': omdb_data.get('Awards'),
            'poster': omdb_data.get('Poster'),
            'ratings': omdb_data.get('Ratings'),
            'imdb_rating': self._parse_rating(omdb_data.get('imdbRating')),
            'imdb_votes': self._parse_votes(omdb_data.get('imdbVotes')),
            'metascore': self._parse_metascore(omdb_data.get('Metascore')),
            'box_office': omdb_data.get('BoxOffice'),
            'production': omdb_data.get('Production'),
            'website': omdb_data.get('Website'),
            'data_source': 'omdb',
            'created_at': datetime.now().isoformat(),
            'updated_at': datetime.now().isoformat()
        }
    
    def _parse_rating(self, rating_str: str) -> Optional[float]:
        """Parse rating string to float"""
        try:
            return float(rating_str) if rating_str and rating_str != 'N/A' else None
        except (ValueError, TypeError):
            return None
    
    def _parse_votes(self, votes_str: str) -> Optional[int]:
        """Parse votes string to integer"""
        try:
            if votes_str and votes_str != 'N/A':
                # Remove commas and convert to int
                return int(votes_str.replace(',', ''))
            return None
        except (ValueError, TypeError):
            return None
    
    def _parse_metascore(self, metascore_str: str) -> Optional[int]:
        """Parse metascore string to integer"""
        try:
            return int(metascore_str) if metascore_str and metascore_str != 'N/A' else None
        except (ValueError, TypeError):
            return None
    
    def scrape_metacritic_ratings(self, movies: List[Dict[str, Any]], max_movies: int = 50) -> List[Dict[str, Any]]:
        """Scrape Metacritic ratings for movies"""
        logger.info(f"Scraping Metacritic ratings for {len(movies)} movies (max: {max_movies})")
        
        metacritic_ratings = []
        
        for i, movie in enumerate(movies[:max_movies]):
            try:
                title = movie.get('title')
                year = movie.get('year')
                
                if not title or not year:
                    continue
                
                logger.info(f"Scraping Metacritic {i+1}/{len(movies[:max_movies])}: {title} ({year})")
                
                rating_data = self.metacritic_scraper.get_movie_rating(title, year)
                if rating_data:
                    rating_data.update({
                        'tmdb_id': movie.get('tmdb_id'),
                        'imdb_id': movie.get('imdb_id'),
                        'title': title,
                        'year': year,
                        'data_source': 'metacritic_scraped',
                        'created_at': datetime.now().isoformat()
                    })
                    metacritic_ratings.append(rating_data)
                    logger.info(f"Successfully scraped Metacritic data for {title}")
                
                # Rate limiting for scraping
                time.sleep(1)  # 1 second between requests
                
            except Exception as e:
                logger.error(f"Error scraping Metacritic for {movie.get('title', 'unknown')}: {e}")
                continue
        
        logger.info(f"Successfully scraped Metacritic ratings for {len(metacritic_ratings)} movies")
        self.staged_data['metacritic_ratings'] = metacritic_ratings
        return metacritic_ratings
    
    def scrape_rotten_tomatoes_ratings(self, movies: List[Dict[str, Any]], max_movies: int = 50) -> List[Dict[str, Any]]:
        """Scrape Rotten Tomatoes ratings for movies"""
        logger.info(f"Scraping Rotten Tomatoes ratings for {len(movies)} movies (max: {max_movies})")
        
        rotten_tomatoes_ratings = []
        
        for i, movie in enumerate(movies[:max_movies]):
            try:
                title = movie.get('title')
                year = movie.get('year')
                
                if not title or not year:
                    continue
                
                logger.info(f"Scraping Rotten Tomatoes {i+1}/{len(movies[:max_movies])}: {title} ({year})")
                
                rating_data = self.rotten_tomatoes_scraper.get_movie_rating(title, year)
                if rating_data:
                    rating_data.update({
                        'tmdb_id': movie.get('tmdb_id'),
                        'imdb_id': movie.get('imdb_id'),
                        'title': title,
                        'year': year,
                        'data_source': 'rotten_tomatoes_scraped',
                        'created_at': datetime.now().isoformat()
                    })
                    rotten_tomatoes_ratings.append(rating_data)
                    logger.info(f"Successfully scraped Rotten Tomatoes data for {title}")
                
                # Rate limiting for scraping
                time.sleep(2)  # 2 seconds between requests
                
            except Exception as e:
                logger.error(f"Error scraping Rotten Tomatoes for {movie.get('title', 'unknown')}: {e}")
                continue
        
        logger.info(f"Successfully scraped Rotten Tomatoes ratings for {len(rotten_tomatoes_ratings)} movies")
        self.staged_data['rotten_tomatoes_ratings'] = rotten_tomatoes_ratings
        return rotten_tomatoes_ratings
    
    def save_staged_data(self) -> bool:
        """Save all staged data to JSON files"""
        logger.info("Saving staged data to files...")
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        success = True
        
        try:
            # Save TMDB movies
            if self.staged_data['tmdb_movies']:
                filename = f"data/raw/tmdb_top_rated_movies_{timestamp}.json"
                with open(filename, 'w', encoding='utf-8') as f:
                    json.dump(self.staged_data['tmdb_movies'], f, indent=2, ensure_ascii=False)
                logger.info(f"Saved {len(self.staged_data['tmdb_movies'])} TMDB movies to {filename}")
            
            # Save OMDb movies
            if self.staged_data['omdb_movies']:
                filename = f"data/raw/omdb_top_rated_movies_{timestamp}.json"
                with open(filename, 'w', encoding='utf-8') as f:
                    json.dump(self.staged_data['omdb_movies'], f, indent=2, ensure_ascii=False)
                logger.info(f"Saved {len(self.staged_data['omdb_movies'])} OMDb movies to {filename}")
            
            # Save Metacritic ratings
            if self.staged_data['metacritic_ratings']:
                filename = f"data/raw/metacritic_top_rated_ratings_{timestamp}.json"
                with open(filename, 'w', encoding='utf-8') as f:
                    json.dump(self.staged_data['metacritic_ratings'], f, indent=2, ensure_ascii=False)
                logger.info(f"Saved {len(self.staged_data['metacritic_ratings'])} Metacritic ratings to {filename}")
            
            # Save Rotten Tomatoes ratings
            if self.staged_data['rotten_tomatoes_ratings']:
                filename = f"data/raw/rotten_tomatoes_top_rated_ratings_{timestamp}.json"
                with open(filename, 'w', encoding='utf-8') as f:
                    json.dump(self.staged_data['rotten_tomatoes_ratings'], f, indent=2, ensure_ascii=False)
                logger.info(f"Saved {len(self.staged_data['rotten_tomatoes_ratings'])} Rotten Tomatoes ratings to {filename}")
            
            logger.info("All staged data saved successfully")
            return True
            
        except Exception as e:
            logger.error(f"Error saving staged data: {e}")
            return False
    
    def run_top_rated_pipeline(self, max_pages: int = 500, max_omdb: int = 100, 
                              max_scraping: int = 50) -> bool:
        """Run the complete top-rated movies pipeline"""
        logger.info("Starting top-rated movies ingestion pipeline...")
        logger.info("=" * 60)
        
        try:
            # Step 1: Get top-rated movies from TMDB
            logger.info("STEP 1: Fetching top-rated movies from TMDB...")
            tmdb_movies = self.get_top_rated_movies(max_pages=max_pages)
            
            if not tmdb_movies:
                logger.error("No TMDB movies fetched. Pipeline failed.")
                return False
            
            # Step 2: Get OMDb data for movies with IMDB IDs
            logger.info("STEP 2: Fetching OMDb data...")
            omdb_movies = self.get_omdb_data_for_movies(tmdb_movies, max_movies=max_omdb)
            
            # Step 3: Scrape Metacritic ratings
            logger.info("STEP 3: Scraping Metacritic ratings...")
            metacritic_ratings = self.scrape_metacritic_ratings(tmdb_movies, max_movies=max_scraping)
            
            # Step 4: Scrape Rotten Tomatoes ratings
            logger.info("STEP 4: Scraping Rotten Tomatoes ratings...")
            rotten_tomatoes_ratings = self.scrape_rotten_tomatoes_ratings(tmdb_movies, max_movies=max_scraping)
            
            # Step 5: Save all data
            logger.info("STEP 5: Saving staged data...")
            if self.save_staged_data():
                logger.info("Pipeline completed successfully!")
                logger.info(f"Check the data/raw/ directory for staged data files")
                return True
            else:
                logger.error("Failed to save staged data")
                return False
                
        except Exception as e:
            logger.error(f"Pipeline failed: {e}")
            return False


def main():
    """Main function for testing"""
    try:
        ingestor = TopRatedMoviesIngestor()
        
        # Run pipeline with reasonable limits
        success = ingestor.run_top_rated_pipeline(
            max_pages=50,      # Start with 50 pages (1000 movies)
            max_omdb=100,      # Get OMDb data for 100 movies
            max_scraping=25    # Scrape ratings for 25 movies
        )
        
        if success:
            print("Top-rated movies pipeline completed successfully!")
        else:
            print("Top-rated movies pipeline failed!")
            
    except Exception as e:
        print(f"Error: {e}")


if __name__ == "__main__":
    main()
