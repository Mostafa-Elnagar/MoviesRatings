# rotten_tomatoes_scraper.py

from bs4 import BeautifulSoup, Tag
from urllib.parse import urljoin
from .base_scraper import logger, PlaywrightScraper
from typing import Optional, Dict
import re

class RottenTomatoesScraper(PlaywrightScraper):
    """
    Scraper for Rotten Tomatoes movie ratings using Playwright.
    """
    def __init__(self, headless: bool = True):
        super().__init__(base_url="https://www.rottentomatoes.com/", headless=headless)

    
    def get_ratings(self, movie_title: str, year: int) -> Optional[Dict[str, int | float | None]]:
        """
        Fetch and parse ratings for a given movie and year. Retries with _{year} suffix if not found or year mismatch.
        """
        return super().get_ratings(movie_title, year, '_')

    def _fetch_and_validate(self, formatted_title: str, year: int) -> Optional[Dict[str, int | float | None]]:
        """
        Fetch page content and validate year match, then parse ratings.
        """
        html_content = self._fetch_page(formatted_title, year)
        if not html_content:
            return {}
        
        ratings = self._parse_content(html_content)
        return ratings

    def _fetch_page(self, formatted_title: str, year: int) -> Optional[str]:
        """
        Override to match the signature expected by PlaywrightScraper.
        """
        return super()._fetch_page(formatted_title, year)

    def _parse_content(self, html_content: str) -> Dict[str, int | float | None]:
        soup = BeautifulSoup(html_content, "html.parser")
        ratings: Dict[str, int | float | None] = {
            "data_source": "rottentomatoes",
            "critic_score": None,
            "critic_count": None,
            "user_score": None,
            "user_count": None
        }
        

        # Extract ratings from media-scorecard
        media_scorecard = soup.find("div", class_="media-scorecard")
        if isinstance(media_scorecard, Tag):
            # Critic score
            critic_score_text = media_scorecard.find("rt-text", attrs={"slot": "criticsScore"})
            if isinstance(critic_score_text, Tag) and critic_score_text.text:
                try:
                    ratings["critic_score"] = float(critic_score_text.text.strip().replace('%', ''))
                except ValueError:
                    pass
            
            # Critic count (number of reviews)
            critic_link = media_scorecard.find("rt-link", attrs={"slot": "criticsReviews"})
            if isinstance(critic_link, Tag) and critic_link.text:
                try:
                    critic_count_match = re.search(r'(\d+)', critic_link.text.strip())
                    if critic_count_match:
                        ratings["critic_count"] = int(critic_count_match.group(1))
                except (ValueError, AttributeError) as e:
                    print(f"criticsLink NotFOUND: {e}")
            
            # Audience score
            audience_score_text = media_scorecard.find("rt-text", attrs={"slot": "audienceScore"})
            if isinstance(audience_score_text, Tag) and audience_score_text.text:
                try:
                    ratings["user_score"] = float(audience_score_text.text.strip().replace('%', ''))
                except ValueError:
                    pass

            # Audience count 
            critic_link = media_scorecard.find("rt-link", attrs={"slot": "audienceReviews"})
            if isinstance(critic_link, Tag) and critic_link.text:
                try:
                    critic_count_match = critic_link.text.replace("Ratings", "").strip()
                    ratings["user_count"] = critic_count_match
                except (ValueError, AttributeError):
                    pass
        

        return ratings