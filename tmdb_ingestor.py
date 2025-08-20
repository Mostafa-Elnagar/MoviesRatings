import os
import requests
from dotenv import load_dotenv
import json
import logging
from typing import Optional, Dict, Any, List
import time

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger(__name__)

project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
env_path = os.path.join(project_root, '.env')
load_dotenv(dotenv_path=env_path)

class TMDBIngestor:
    """
    Ingests comprehensive movie metadata from the TMDB API.
    """
    BASE_URL = "https://api.themoviedb.org/3"

    def __init__(self, api_key: str | None = None):
        self.api_key = api_key or os.getenv("TMDB_API_KEY")
        if not self.api_key:
            raise ValueError("TMDB API key must be set in TMDB_API_KEY environment variable or passed explicitly.")
        logger.info("TMDBIngestor initialized with API key.")
        self.movie_genre_map = self._fetch_movie_genre_map()

    def _fetch_movie_genre_map(self, language: str = "en-US"):
        """Fetch movie genre mapping."""
        url = f"{self.BASE_URL}/genre/movie/list"
        params = {
            "api_key": self.api_key,
            "language": language
        }
        logger.info(f"Fetching movie genre map from {url}")
        response = requests.get(url, params=params)
        response.raise_for_status()
        genres = response.json().get("genres", [])
        logger.info(f"Fetched {len(genres)} movie genres.")
        return {genre["id"]: genre["name"] for genre in genres}

    def fetch_top_rated_movies(self, language: str = "en-US", max_pages: Optional[int] = 5, include_details: bool = True):
        """
        Fetches all top-rated movies from TMDB with comprehensive data.
        """
        url = f"{self.BASE_URL}/movie/top_rated"
        page = 1
        all_movies = []
        logger.info("Starting to fetch top-rated movies from TMDB.")
        
        while True:
            params = {
                "api_key": self.api_key,
                "language": language,
                "page": page
            }
            logger.info(f"Fetching page {page} from {url}")
            response = requests.get(url, params=params)
            response.raise_for_status()
            data = response.json()
            results = data.get("results", [])
            
            if not results:
                logger.info("No more results found, stopping pagination.")
                break
                
            for item in results:
                movie_data = self._parse_movie_basic(item)
                
                if include_details:
                    # Fetch detailed information
                    detailed_data = self._fetch_movie_details(item.get("id"), language)
                    if detailed_data:
                        movie_data.update(detailed_data)
                
                all_movies.append(movie_data)
                
                # Rate limiting
                time.sleep(0.1)
            
            logger.info(f"Fetched {len(results)} movies from page {page}.")
            total_pages = data.get("total_pages", page)
            
            if max_pages and page >= max_pages:
                logger.info(f"Reached max_pages limit: {max_pages}.")
                break
            if page >= total_pages:
                logger.info("Reached the last available page.")
                break
            page += 1
            
        logger.info(f"Total movies fetched: {len(all_movies)}")
        return all_movies

    def fetch_popular_movies(self, language: str = "en-US", max_pages: Optional[int] = 5):
        """
        Fetch popular movies.
        
        Args:
            language: Language for results
            max_pages: Maximum pages to fetch
        """
        url = f"{self.BASE_URL}/movie/popular"
        page = 1
        all_movies = []
        
        while True:
            params = {
                "api_key": self.api_key,
                "language": language,
                "page": page
            }
            logger.info(f"Fetching popular movies page {page}")
            response = requests.get(url, params=params)
            response.raise_for_status()
            data = response.json()
            results = data.get("results", [])
            
            if not results or (max_pages and page >= max_pages):
                break
                
            for item in results:
                content_data = self._parse_movie_basic(item)
                all_movies.append(content_data)
            
            page += 1
            time.sleep(0.1)
            
        return all_movies

    def search_movies(self, query: str, language: str = "en-US", year: Optional[int] = None):
        """
        Search for movies.
        
        Args:
            query: Search query
            language: Language for results
            year: Filter by year
        """
        url = f"{self.BASE_URL}/search/movie"
        params = {
            "api_key": self.api_key,
            "query": query,
            "language": language,
            "include_adult": False
        }
        
        if year:
            params["year"] = year
            
        response = requests.get(url, params=params)
        response.raise_for_status()
        data = response.json()
        
        results = data.get("results", [])
        return [self._parse_movie_basic(item) for item in results]

    def _parse_movie_basic(self, item: Dict[str, Any]) -> Dict[str, Any]:
        """Parse basic movie information."""
        genre_names = [self.movie_genre_map.get(gid, str(gid)) for gid in item.get("genre_ids", [])]
        return {
            "data_source": "tmdb",
            "title": item.get("title"),
            "original_title": item.get("original_title"),
            "year": int(item.get("release_date", "0000")[:4]) if item.get("release_date") else None,
            "release_date": item.get("release_date"),
            "genres": genre_names,
            "genre_ids": item.get("genre_ids", []),
            "language": item.get("original_language"),
            "overview": item.get("overview"),
            "tmdb_id": item.get("id"),
            "popularity": item.get("popularity"),
            "vote_average": item.get("vote_average"),
            "vote_count": item.get("vote_count"),
            "poster_path": item.get("poster_path"),
            "backdrop_path": item.get("backdrop_path"),
        }

    def _fetch_movie_details(self, movie_id: int, language: str = "en-US") -> Optional[Dict[str, Any]]:
        """Fetch detailed movie information."""
        try:
            # Movie details
            details_url = f"{self.BASE_URL}/movie/{movie_id}"
            params = {"api_key": self.api_key, "language": language}
            response = requests.get(details_url, params=params)
            response.raise_for_status()
            details = response.json()
            
            # Credits (cast & crew)
            credits_url = f"{self.BASE_URL}/movie/{movie_id}/credits"
            response = requests.get(credits_url, params=params)
            credits = response.json() if response.status_code == 200 else {}
            
            return {
                "data_source": "tmdb",
                "status": details.get("status"),
                "runtime": details.get("runtime"),
                "budget": details.get("budget"),
                "revenue": details.get("revenue"),
                "production_companies": details.get("production_companies", []),
                "production_countries": details.get("production_countries", []),
                "spoken_languages": details.get("spoken_languages", []),
                "cast": credits.get("cast", []),
                "crew": credits.get("crew", []),
                "external_ids": details.get("external_ids", {}),
                "tagline": details.get("tagline"),
                "belongs_to_collection": details.get("belongs_to_collection"),
                "imdb_id": details.get("imdb_id")
            }
            
        except Exception as e:
            logger.error(f"Failed to fetch movie details for ID {movie_id}: {e}")
            return None

    def get_movie_by_id(self, movie_id: int, language: str = "en-US") -> Optional[Dict[str, Any]]:
        """
        Get movie by TMDB ID with full details.
        
        Args:
            movie_id: TMDB movie ID
            language: Language for results
        """
        return self._fetch_movie_details(movie_id, language)

if __name__ == "__main__":
    tmdb_ingestor = TMDBIngestor()
    
    # Fetch top-rated movies with details
    logger.info("Fetching top-rated movies...")
    movie_list = tmdb_ingestor.fetch_top_rated_movies(language="en-US", max_pages=2, include_details=True)
    
    # Save movie data
    with open("tmdb_movies_detailed.json", "w", encoding="utf-8") as f:
        json.dump(movie_list, f, indent=2, ensure_ascii=False)
    
    print(f"Total movies: {len(movie_list)}")
    print("Data saved to tmdb_movies_detailed.json")