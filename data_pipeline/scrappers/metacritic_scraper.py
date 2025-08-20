from bs4 import BeautifulSoup
from urllib.parse import urljoin
from .base_scraper import HtmlScraper, logger
import re
from typing import Optional, Dict

class MetacriticScraper(HtmlScraper):
    """
    Scraper for Metacritic movie ratings.
    """
    def __init__(self):
        super().__init__(base_url="https://www.metacritic.com/")

    def get_ratings(self, movie_title: str, year: int) -> Optional[Dict[str, int | float | None]]:
        """
        Fetch and parse ratings for a given movie and year. 
        Retries with -{year} suffix if year mismatch.
        """
        return super().get_ratings(movie_title, year, '-')

    def _fetch_and_validate(self, formatted_title: str, year: int) -> Optional[Dict[str, int | float | None]]:
        url = urljoin(self.base_url, f"movie/{formatted_title}")
        html_content: Optional[str] = self._fetch_page(url)
        if not html_content:
            return {}
        ratings = self._parse_content(html_content)
        scraped_year = int(ratings.get("year"))

        if scraped_year and abs(scraped_year - year) > 3: # Integrity check
            logger.warning(f"Year mismatch for {formatted_title}: expected {year}, found {scraped_year}.")
            return {}
        
        return ratings

    def _parse_content(self, html_content: str) -> Dict[str, int | float | None]:
        soup = BeautifulSoup(html_content, 'html.parser')
        ratings: Dict[str, int | float | None] = {
            "data_source": "metacritic",
            "critic_score": None,
            "critic_count": None,
            "user_score": None,
            "user_count": None
        }

        # Extract year
        hero_metadata = soup.find("div", attrs={"data-testid": "hero-metadata"})
        try:
            ratings["year"] = int(hero_metadata.find("li").find("span").text.strip()) # type: ignore
        except (AttributeError, ValueError):
            raise ValueError("Could not extract year")
        
        # Extract critic info
        critic_info = soup.find("div", attrs={"data-testid": "critic-score-info"})
        try:
            ratings["critic_score"] = float(critic_info.find("div", class_="c-siteReviewScore").find("span").text.strip()) # type: ignore
        except (AttributeError, ValueError):
            ratings["critic_score"] = None
            
        try:
            review_span = critic_info.find("a", attrs={"data-testid": "critic-path"}).get_text(strip=True)
            match = re.search(r"(\d+)", review_span)
            ratings["critic_count"] = int(match.group(1).replace(",", ""))
        except (AttributeError, ValueError):
            ratings["critic_count"] = None

        # Extract user info
        user_info = soup.find("div", attrs={"data-testid": "user-score-info"})

        try:
            ratings["user_score"] = float(user_info.find("div", class_="c-siteReviewScore").find("span").text.strip())
        except (AttributeError, ValueError):
            ratings["user_score"] = None

        try:
            review_span = user_info.find("a", attrs={"data-testid": "user-path"}).get_text(strip=True)
            match = re.search(r"([\d,]+)", review_span)
            ratings["user_count"] = int(match.group(1).replace(",", ""))
        except Exception:
            ratings["user_count"] = None

        return ratings
