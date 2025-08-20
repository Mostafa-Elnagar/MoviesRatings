# main.py
from .metacritic_scraper import MetacriticScraper
from .tomatos_scraper import RottenTomatoesScraper
from .base_scraper import logger
from typing import List, Tuple, Optional, Dict

def print_ratings(source: str, scraper, series_list: List[Tuple[str, int]]):
    """
    Generic function to print ratings for a list of series using a given scraper.
    """
    logger.info(f"--- {source} Scraping ---")
    for title, year in series_list:
        logger.info(f"Scraping {source} for: {title} ({year})")
        ratings: Optional[Dict] = scraper.get_ratings(title, year)
        if ratings:
            logger.info(f"{source} Ratings for {title}:")
            for key, value in ratings.items():
                logger.info(f"  {key.replace('_', ' ').title()}: {value}")
        else:
            logger.warning(f"Failed to get {source} ratings for {title}.")

def main():
    """
    Run both Metacritic and Rotten Tomatoes scrapers on a test list of series.
    """
    test_series = [
        ("Game of Thrones", 2011),
        ("The Last of Us", 2023),
        ("The Boys", 2019),
        ("Series That Does Not Exist", 2024),
        ("Stranger Things Season 5", 2025) # doesn't exist
    ]
    print_ratings("Metacritic", MetacriticScraper(), test_series)
    logger.info("="*50)
    with RottenTomatoesScraper() as rt_scraper:
        print_ratings("Rotten Tomatoes", rt_scraper, test_series)

if __name__ == "__main__":
    main()