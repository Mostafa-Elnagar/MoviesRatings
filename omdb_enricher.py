import os
import requests

class OMDbEnricher:
    """
    Enriches series metadata with OMDb ratings.
    """
    BASE_URL = "http://www.omdbapi.com/"

    def __init__(self, api_key: str | None = None):
        self.api_key = api_key or os.getenv("OMDB_API_KEY")
        if not self.api_key:
            raise ValueError("OMDb API key must be set in OMDB_API_KEY environment variable or passed explicitly.")

    def get_ratings(self, title: str, year: int):
        params = {
            "apikey": self.api_key,
            "t": title,
            "y": year,
            "type": "series",
        }
        # if year:
        #     params["y"] = year
        resp = requests.get(self.BASE_URL, params=params)
        if resp.status_code != 200:
            return {
                "title": title,
                "year": year,
                "imdb_rating": None,
                "imdb_count": None,
            }
        data = resp.json()
        if data.get("Response") != "True":
            return {
                "title": title,
                "year": year,
                "imdb_rating": None,
                "imdb_count": None,
            }
        return {
            "title": title,
            "year": year,
            "imdb_rating": float(data.get("imdbRating", 0)) if data.get("imdbRating") else None,
            "imdb_count": int(data.get("imdbVotes", "0").replace(",", "")) if data.get("imdbVotes") else None,
        }