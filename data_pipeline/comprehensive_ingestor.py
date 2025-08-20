#!/usr/bin/env python3
"""
Comprehensive Data Ingestion Pipeline for Movie Ratings Data Lakehouse
- Gets TMDB data by IMDB ID
- Scrapes Rotten Tomatoes and Metacritic data
- Stages all data in the data directory
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
        logging.FileHandler("logs/comprehensive_ingestion.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class ComprehensiveMovieIngestor:
    """Comprehensive movie data ingestion from multiple sources"""
    
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
        from .scrappers.metacritic_scraper import MetacriticScraper
        from .scrappers.tomatos_scraper import RottenTomatoesScraper
        
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
    
    def get_tmdb_movies_by_imdb_ids(self, imdb_ids: List[str], max_movies: int = 100) -> List[Dict[str, Any]]:
        """Get TMDB movie data by IMDB IDs"""
        logger.info(f"Fetching TMDB data for {len(imdb_ids)} IMDB IDs (max: {max_movies})")
        
        movies = []
        for i, imdb_id in enumerate(imdb_ids[:max_movies]):
            try:
                logger.info(f"Processing IMDB ID {i+1}/{len(imdb_ids[:max_movies])}: {imdb_id}")
                
                # Search TMDB by IMDB ID
                search_url = f"{self.tmdb_base_url}/find/{imdb_id}"
                params = {
                    'api_key': self.tmdb_api_key,
                    'external_source': 'imdb_id',
                    'language': 'en-US'
                }
                
                response = requests.get(search_url, params=params, timeout=10)
                response.raise_for_status()
                data = response.json()
                
                movie_results = data.get('movie_results', [])
                if movie_results:
                    movie = movie_results[0]  # Take the first result
                    
                    # Get detailed movie info
                    movie_id = movie['id']
                    details_url = f"{self.tmdb_base_url}/movie/{movie_id}"
                    details_params = {
                        'api_key': self.tmdb_api_key,
                        'language': 'en-US',
                        'append_to_response': 'credits,external_ids'
                    }
                    
                    details_response = requests.get(details_url, params=details_params, timeout=10)
                    details_response.raise_for_status()
                    movie_details = details_response.json()
                    
                    # Get genre mapping
                    genres_url = f"{self.tmdb_base_url}/genre/movie/list"
                    genres_params = {'api_key': self.tmdb_api_key, 'language': 'en-US'}
                    genres_response = requests.get(genres_url, params=genres_params, timeout=10)
                    genres_response.raise_for_status()
                    genres_data = genres_response.json()
                    genre_map = {g['id']: g['name'] for g in genres_data.get('genres', [])}
                    
                    # Process movie data
                    processed_movie = self._process_tmdb_movie(movie_details, genre_map, imdb_id)
                    movies.append(processed_movie)
                    
                    logger.info(f"Successfully processed {processed_movie.get('title', 'Unknown')}")
                else:
                    logger.warning(f"No TMDB results found for IMDB ID: {imdb_id}")
                
                # Rate limiting
                time.sleep(0.2)
                
            except Exception as e:
                logger.error(f"Error processing IMDB ID {imdb_id}: {e}")
                continue
        
        logger.info(f"Successfully processed {len(movies)} movies from TMDB")
        self.staged_data['tmdb_movies'] = movies
        return movies
    
    def _process_tmdb_movie(self, movie: Dict[str, Any], genre_map: Dict[int, str], imdb_id: str) -> Dict[str, Any]:
        """Process TMDB movie data into standardized format"""
        # Extract genres
        genres = [genre_map.get(g['id'], str(g['id'])) for g in movie.get('genres', [])]
        genre_ids = [g['id'] for g in movie.get('genres', [])]
        
        # Extract cast and crew
        credits = movie.get('credits', {})
        cast = [{'id': p['id'], 'name': p['name'], 'character': p.get('character', '')} 
                for p in credits.get('cast', [])]  # Top 10 cast members
        crew = [{'id': p['id'], 'name': p['name'], 'job': p.get('job', '')} 
                for p in credits.get('crew', [])]  # Top 10 crew members
        
        # Extract external IDs
        external_ids = movie.get('external_ids', {})
        
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
            'data_source': 'tmdb',
            'created_at': datetime.now().isoformat(),
            'updated_at': datetime.now().isoformat()
        }
    
    def get_omdb_data_for_movies(self, movies: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Get OMDb data for movies"""
        logger.info(f"Fetching OMDb data for {len(movies)} movies")
        
        omdb_movies = []
        for i, movie in enumerate(movies):
            try:
                title = movie.get('title')
                year = movie.get('year')
                imdb_id = movie.get('imdb_id')
                
                logger.info(f"Processing OMDb data {i+1}/{len(movies)}: {title} ({year})")
                
                # Get OMDb data
                omdb_data = self._fetch_omdb_data(title, year, imdb_id)
                if omdb_data:
                    omdb_movies.append(omdb_data)
                
                # Rate limiting
                time.sleep(0.2)
                
            except Exception as e:
                logger.error(f"Error processing OMDb data for {movie.get('title', 'Unknown')}: {e}")
                continue
        
        logger.info(f"Successfully processed {len(omdb_movies)} movies from OMDb")
        self.staged_data['omdb_movies'] = omdb_movies
        return omdb_movies
    
    def _fetch_omdb_data(self, title: str, year: int, imdb_id: str) -> Optional[Dict[str, Any]]:
        """Fetch OMDb data for a movie"""
        params = {
            'apikey': self.omdb_api_key,
            'plot': 'full'
        }
        
        if imdb_id:
            params['i'] = imdb_id
        else:
            params['t'] = title
            params['y'] = year
        
        try:
            response = requests.get(self.omdb_base_url, params=params, timeout=10)
            response.raise_for_status()
            data = response.json()
            
            if data.get('Response') == 'True':
                return self._process_omdb_movie(data, title, year, imdb_id)
            else:
                logger.warning(f"OMDb API error for {title}: {data.get('Error', 'Unknown error')}")
                return None
                
        except Exception as e:
            logger.error(f"Error fetching OMDb data for {title}: {e}")
            return None
    
    def _process_omdb_movie(self, data: Dict[str, Any], title: str, year: int, imdb_id: str) -> Dict[str, Any]:
        """Process OMDb movie data into standardized format"""
        # Parse ratings
        ratings = {}
        for rating in data.get('Ratings', []):
            source = rating.get('Source', '').lower()
            value = rating.get('Value', '')
            if source and value:
                ratings[source] = value
        
        # Parse numeric values
        imdb_rating = None
        imdb_votes = None
        metascore = None
        
        if data.get('imdbRating') and data.get('imdbRating') != 'N/A':
            try:
                imdb_rating = float(data['imdbRating'])
            except ValueError:
                pass
        
        if data.get('imdbVotes') and data.get('imdbVotes') != 'N/A':
            try:
                imdb_votes = int(data['imdbVotes'].replace(',', ''))
            except ValueError:
                pass
        
        if data.get('Metascore') and data.get('Metascore') != 'N/A':
            try:
                metascore = int(data['Metascore'])
            except ValueError:
                pass
        
        return {
            'imdb_id': imdb_id,
            'title': title,
            'year': year,
            'omdb_title': data.get('Title'),
            'rated': data.get('Rated'),
            'released': data.get('Released'),
            'runtime': data.get('Runtime'),
            'genre': data.get('Genre'),
            'director': data.get('Director'),
            'writer': data.get('Writer'),
            'actors': data.get('Actors'),
            'plot': data.get('Plot'),
            'language': data.get('Language'),
            'country': data.get('Country'),
            'awards': data.get('Awards'),
            'poster': data.get('Poster'),
            'ratings': ratings,
            'imdb_rating': imdb_rating,
            'imdb_votes': imdb_votes,
            'metascore': metascore,
            'box_office': data.get('BoxOffice'),
            'production': data.get('Production'),
            'website': data.get('Website'),
            'data_source': 'omdb',
            'created_at': datetime.now().isoformat(),
            'updated_at': datetime.now().isoformat()
        }
    
    def scrape_metacritic_ratings(self, movies: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Scrape Metacritic ratings for movies"""
        logger.info(f"Scraping Metacritic ratings for {len(movies)} movies")
        
        metacritic_ratings = []
        for i, movie in enumerate(movies):
            try:
                title = movie.get('title')
                year = movie.get('year')
                
                if not title or not year:
                    continue
                
                logger.info(f"Scraping Metacritic {i+1}/{len(movies)}: {title} ({year})")
                
                # Scrape Metacritic
                ratings = self.metacritic_scraper.get_ratings(title, year)
                if ratings:
                    ratings['tmdb_id'] = movie.get('tmdb_id')
                    ratings['imdb_id'] = movie.get('imdb_id')
                    ratings['title'] = title
                    ratings['year'] = year
                    ratings['created_at'] = datetime.now().isoformat()
                    metacritic_ratings.append(ratings)
                
                # Rate limiting
                time.sleep(1)
                
            except Exception as e:
                logger.error(f"Error scraping Metacritic for {movie.get('title', 'Unknown')}: {e}")
                continue
        
        logger.info(f"Successfully scraped Metacritic ratings for {len(metacritic_ratings)} movies")
        self.staged_data['metacritic_ratings'] = metacritic_ratings
        return metacritic_ratings
    
    def scrape_rotten_tomatoes_ratings(self, movies: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Scrape Rotten Tomatoes ratings for movies"""
        logger.info(f"Scraping Rotten Tomatoes ratings for {len(movies)} movies")
        
        rotten_tomatoes_ratings = []
        for i, movie in enumerate(movies):
            try:
                title = movie.get('title')
                year = movie.get('year')
                
                if not title or not year:
                    continue
                
                logger.info(f"Scraping Rotten Tomatoes {i+1}/{len(movies)}: {title} ({year})")
                
                # Scrape Rotten Tomatoes
                ratings = self.rotten_tomatoes_scraper.get_ratings(title, year)
                if ratings:
                    ratings['tmdb_id'] = movie.get('tmdb_id')
                    ratings['imdb_id'] = movie.get('imdb_id')
                    ratings['title'] = title
                    ratings['year'] = year
                    ratings['created_at'] = datetime.now().isoformat()
                    rotten_tomatoes_ratings.append(ratings)
                
                # Rate limiting
                time.sleep(1)
                
            except Exception as e:
                logger.error(f"Error scraping Rotten Tomatoes for {movie.get('title', 'Unknown')}: {e}")
                continue
        
        logger.info(f"Successfully scraped Rotten Tomatoes ratings for {len(rotten_tomatoes_ratings)} movies")
        self.staged_data['rotten_tomatoes_ratings'] = rotten_tomatoes_ratings
        return rotten_tomatoes_ratings
    
    def save_staged_data(self) -> bool:
        """Save all staged data to files"""
        logger.info("Saving staged data to files")
        
        try:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            
            # Save TMDB movies
            if self.staged_data['tmdb_movies']:
                filename = f"data/raw/tmdb_movies_{timestamp}.json"
                with open(filename, 'w', encoding='utf-8') as f:
                    json.dump(self.staged_data['tmdb_movies'], f, indent=2, ensure_ascii=False)
                logger.info(f"Saved {len(self.staged_data['tmdb_movies'])} TMDB movies to {filename}")
            
            # Save OMDb movies
            if self.staged_data['omdb_movies']:
                filename = f"data/raw/omdb_movies_{timestamp}.json"
                with open(filename, 'w', encoding='utf-8') as f:
                    json.dump(self.staged_data['omdb_movies'], f, indent=2, ensure_ascii=False)
                logger.info(f"Saved {len(self.staged_data['omdb_movies'])} OMDb movies to {filename}")
            
            # Save Metacritic ratings
            if self.staged_data['metacritic_ratings']:
                filename = f"data/raw/metacritic_ratings_{timestamp}.json"
                with open(filename, 'w', encoding='utf-8') as f:
                    json.dump(self.staged_data['metacritic_ratings'], f, indent=2, ensure_ascii=False)
                logger.info(f"Saved {len(self.staged_data['metacritic_ratings'])} Metacritic ratings to {filename}")
            
            # Save Rotten Tomatoes ratings
            if self.staged_data['rotten_tomatoes_ratings']:
                filename = f"data/raw/rotten_tomatoes_ratings_{timestamp}.json"
                with open(filename, 'w', encoding='utf-8') as f:
                    json.dump(self.staged_data['rotten_tomatoes_ratings'], f, indent=2, ensure_ascii=False)
                logger.info(f"Saved {len(self.staged_data['rotten_tomatoes_ratings'])} Rotten Tomatoes ratings to {filename}")
            
            return True
            
        except Exception as e:
            logger.error(f"Error saving staged data: {e}")
            return False
    
    def run_ingestion_pipeline(self, imdb_ids: List[str], max_movies: int = 50) -> bool:
        """Run the complete ingestion pipeline"""
        logger.info("Starting Comprehensive Movie Data Ingestion Pipeline")
        logger.info("=" * 60)
        
        try:
            # Step 1: Get TMDB data by IMDB IDs
            tmdb_movies = self.get_tmdb_movies_by_imdb_ids(imdb_ids, max_movies)
            if not tmdb_movies:
                logger.error("No TMDB movies retrieved")
                return False
            
            # Step 2: Get OMDb data
            omdb_movies = self.get_omdb_data_for_movies(tmdb_movies)
            
            # Step 3: Scrape Metacritic ratings
            metacritic_ratings = self.scrape_metacritic_ratings(tmdb_movies)
            
            # Step 4: Scrape Rotten Tomatoes ratings
            rotten_tomatoes_ratings = self.scrape_rotten_tomatoes_ratings(tmdb_movies)
            
            # Step 5: Save all staged data
            if not self.save_staged_data():
                logger.error("Failed to save staged data")
                return False
            
            logger.info("=" * 60)
            logger.info("Data Ingestion Pipeline Completed Successfully!")
            logger.info(f"TMDB movies: {len(tmdb_movies)}")
            logger.info(f"OMDb movies: {len(omdb_movies)}")
            logger.info(f"Metacritic ratings: {len(metacritic_ratings)}")
            logger.info(f"Rotten Tomatoes ratings: {len(rotten_tomatoes_ratings)}")
            logger.info("=" * 60)
            
            return True
            
        except Exception as e:
            logger.error(f"Pipeline execution failed: {e}")
            return False


def main():
    """Main function for testing"""
    # Sample IMDB IDs for testing (top movies)
    sample_imdb_ids = [
        "tt0111161",  # The Shawshank Redemption
        "tt0068646",  # The Godfather
        "tt0468569",  # The Dark Knight
        "tt0071562",  # The Godfather Part II
        "tt0050083"   # 12 Angry Men
    ]
    
    try:
        ingestor = ComprehensiveMovieIngestor()
        success = ingestor.run_ingestion_pipeline(sample_imdb_ids, max_movies=5)
        
        if success:
            print("\nData ingestion completed successfully!")
            print("Check the data/raw/ directory for staged data files")
        else:
            print("\nData ingestion failed. Check logs for details.")
            
    except Exception as e:
        print(f"Error: {e}")


if __name__ == "__main__":
    main()
