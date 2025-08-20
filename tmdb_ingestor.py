import os
import requests
from dotenv import load_dotenv
import json
import logging

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
    Ingests top-rated TV series metadata from the TMDB API.
    """
    BASE_URL = "https://api.themoviedb.org/3"

    def __init__(self, api_key: str | None = None):
        self.api_key = api_key or os.getenv("TMDB_API_KEY")
        if not self.api_key:
            raise ValueError("TMDB API key must be set in TMDB_API_KEY environment variable or passed explicitly.")
        logger.info("TMDBIngestor initialized with API key.")
        self.genre_map = self._fetch_genre_map()

    def _fetch_genre_map(self, language: str = "en-US"):
        url = f"{self.BASE_URL}/genre/tv/list"
        params = {
            "api_key": self.api_key,
            "language": language
        }
        logger.info(f"Fetching genre map from {url}")
        response = requests.get(url, params=params)
        response.raise_for_status()
        genres = response.json().get("genres", [])
        logger.info(f"Fetched {len(genres)} genres.")
        return {genre["id"]: genre["name"] for genre in genres}

    def fetch_top_rated_series(self, language: str = "en-US", max_pages: int | None = 5):
        """
        Fetches all top-rated TV series from TMDB, paginating through all available pages
        or up to max_pages if specified.
        """
        url = f"{self.BASE_URL}/tv/top_rated"
        page = 1
        all_series = []
        logger.info("Starting to fetch top-rated series from TMDB.")
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
                genre_names = [self.genre_map.get(gid, str(gid)) for gid in item.get("genre_ids", [])]
                all_series.append({
                    "title": item.get("name"),
                    "year": int(item.get("first_air_date", "0000")[:4]) if item.get("first_air_date") else None,
                    "genres": genre_names,
                    "language": item.get("original_language"),
                    "overview": item.get("overview"),
                    "tmdb_id": item.get("id"),
                    "popularity": item.get("popularity"),
                    "vote_average": item.get("vote_average"),
                    "vote_count": item.get("vote_count"),
                })
            logger.info(f"Fetched {len(results)} series from page {page}.")
            total_pages = data.get("total_pages", page)
            if max_pages and page >= max_pages:
                logger.info(f"Reached max_pages limit: {max_pages}.")
                break
            if page >= total_pages:
                logger.info("Reached the last available page.")
                break
            page += 1
        logger.info(f"Total series fetched: {len(all_series)}")
        return all_series

if __name__ == "__main__":
    tmdb_ingestor = TMDBIngestor()
    series_list = tmdb_ingestor.fetch_top_rated_series(language="en-US")

    with open("tmdb_series.json", "w", encoding="utf-8") as f:
        json.dump(series_list, f, indent=2, ensure_ascii=False)
    print(f"Total series: {len(series_list)}")