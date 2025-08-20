import os
import requests
import logging
from typing import Optional, Dict, Any, List

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class OMDbEnricher:
    """
    Enriches movie and series metadata with comprehensive OMDb data.
    """
    BASE_URL = "https://www.omdbapi.com/"

    def __init__(self, api_key: str | None = None):
        self.api_key = api_key or os.getenv("OMDB_API_KEY")
        if not self.api_key:
            raise ValueError("OMDb API key must be set in OMDB_API_KEY environment variable or passed explicitly.")

    def get_movie_data(self, title: str, year: int, imdb_id: Optional[str] = None, plot: str = "short") -> Dict[str, Any]:
        """
        Get comprehensive movie data from OMDb API.
        
        Args:
            title: Movie title
            year: Release year (optional)
            imdb_id: IMDB ID (optional, more reliable than title/year)
            plot: Plot length - 'short' or 'full'
        """
        params = {
            "apikey": self.api_key,
            "plot": plot,
        }
        
        if imdb_id:
            params["i"] = imdb_id
        else:
            params["t"] = title
            params["y"] = year

        try:
            resp = requests.get(self.BASE_URL, params=params)
            resp.raise_for_status()
            data = resp.json()
            
            if data.get("Response") != "True":
                logger.warning(f"OMDb API error for {title}: {data.get('Error', 'Unknown error')}")
                return self._create_empty_response(title, year)
            
            return self._parse_movie_data(data)
            
        except requests.RequestException as e:
            logger.error(f"Request failed for {title}: {e}")
            return self._create_empty_response(title, year)
        except Exception as e:
            logger.error(f"Unexpected error for {title}: {e}")
            return self._create_empty_response(title, year)

    def search_movies(self, query: str, year:  int, type_filter: Optional[str] = None) -> List[Dict[str, Any]]:
        """
        Search for movies/series by title.
        
        Args:
            query: Search query
            year: Filter by year
            type_filter: 'movie', 'series', or 'episode'
        """
        params = {
            "apikey": self.api_key,
            "s": query,
        }
        
        if year:
            params["y"] = year
        if type_filter:
            params["type"] = type_filter

        try:
            resp = requests.get(self.BASE_URL, params=params)
            resp.raise_for_status()
            data = resp.json()
            
            if data.get("Response") != "True":
                logger.warning(f"OMDb search error: {data.get('Error', 'Unknown error')}")
                return []
            
            return data.get("Search", [])
            
        except Exception as e:
            logger.error(f"Search failed for '{query}': {e}")
            return []

    def _parse_movie_data(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Parse and structure movie data from OMDb API response."""
        return {
            "data_source": "omdb",
            "title": data.get("Title"),
            "year": int(data.get("Year", "0")) if data.get("Year") and data.get("Year").isdigit() else None,
            "rated": data.get("Rated"),
            "released": data.get("Released"),
            "runtime": data.get("Runtime"),
            "genre": data.get("Genre"),
            "director": data.get("Director"),
            "writer": data.get("Writer"),
            "actors": data.get("Actors"),
            "plot": data.get("Plot"),
            "language": data.get("Language"),
            "country": data.get("Country"),
            "awards": data.get("Awards"),
            "ratings": self._parse_ratings(data.get("Ratings", [])),
            "metascore": self._parse_metascore(data.get("Metascore")),
            "imdb_rating": float(data.get("imdbRating", 0)) if data.get("imdbRating") and data.get("imdbRating") != "N/A" else None,
            "imdb_votes": int(data.get("imdbVotes", "0").replace(",", "")) if data.get("imdbVotes") and data.get("imdbVotes") != "N/A" else None,
            "imdb_id": data.get("imdbID"),
            "type": data.get("Type"),
            "dvd": data.get("DVD"),
            "box_office": data.get("BoxOffice"),
            "production": data.get("Production"),
            "website": data.get("Website"),
            "response": data.get("Response") == "True"
        }


    def _parse_ratings(self, ratings: List[Dict[str, str]]) -> Dict[str, Any]:
        """Parse ratings from various sources."""
        parsed_ratings = {}
        for rating in ratings:
            source = rating.get("Source", "").lower()
            value = rating.get("Value", "")
            if source and value:
                parsed_ratings[source] = value
        return parsed_ratings

    def _parse_metascore(self, metascore: Optional[str]) -> Optional[int]:
        """Parse Metascore to integer."""
        if metascore and metascore.isdigit():
            return int(metascore)
        return None

    def _create_empty_response(self, title: str, year: Optional[int], is_series: bool = False) -> Dict[str, Any]:
        """Create empty response structure when API call fails."""
        base_response = {
            "title": title,
            "year": year,
            "response": False,
            "imdb_rating": None,
            "imdb_votes": None,
            "metascore": None,
            "ratings": {},
        }
        
        if is_series:
            base_response.update({
                "total_seasons": None,
                "series_type": "series"
            })
        
        return base_response

    def get_ratings(self, title: str, year: int):
        """
        Legacy method for backward compatibility.
        Returns basic rating information.
        """
        data = self.get_movie_data(title, year)
        return {
            "title": data["title"],
            "year": data["year"],
            "imdb_rating": data["imdb_rating"],
            "imdb_count": data["imdb_votes"],
        }