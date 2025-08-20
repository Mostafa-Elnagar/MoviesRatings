# base_scraper.py
import os
import urllib.robotparser
from urllib.parse import urljoin, urlparse
from abc import ABC, abstractmethod
import requests
from playwright.sync_api import sync_playwright, Page, Browser, BrowserContext
from typing import Optional, Dict
import time
import logging
import re
import unicodedata
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(levelname)s %(message)s',
)
logger = logging.getLogger("scraper")

class BaseScraper(ABC):
    """
    Abstract base class for all scrapers.
    Handles robots.txt and user agent logic.
    """
    DEFAULT_USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"

    def __init__(self, base_url: str, robots_txt_path: str = "robots.txt", user_agent: str = ""):
        if not base_url.endswith('/'):
            base_url += '/'
        self.base_url = base_url
        self.robots_txt_url = urljoin(self.base_url, robots_txt_path or "robots.txt")
        self.user_agent = user_agent or self.DEFAULT_USER_AGENT
        self.robot_parser = urllib.robotparser.RobotFileParser()
        self._load_robots_txt()

    def _load_robots_txt(self) -> None:
        try:
            self.robot_parser.set_url(self.robots_txt_url)
            self.robot_parser.read()
            logger.info(f"Successfully loaded robots.txt from {self.robots_txt_url}")
        except Exception as e:
            logger.warning(f"Error loading robots.txt from {self.robots_txt_url}: {e}. Proceeding without robots.txt rules enforced (not recommended).")

    def is_scraping_allowed(self, url: str) -> bool:
        parsed_url = urlparse(url)
        return self.robot_parser.can_fetch(self.user_agent, parsed_url.path)
    
    def get_ratings(self, movie_title: str, year: int, sep: str) -> Optional[Dict[str, int | float | None]]:
        """
        Fetch and parse ratings for a given movie and year. Retries with _{year} suffix if not found or year mismatch.
        """
        if not movie_title:
            logger.error("Movie title cannot be empty.")
            return {}
        formatted_title = self._preprocess_title(movie_title, sep=sep)
        result = self._fetch_and_validate(formatted_title, year)

        result['title'] = movie_title # type: ignore
        result['year'] = year

        return result
    
    def _preprocess_title(self, title: str, sep: str) -> str:
        """Convert title to a URL-safe slug."""
        title = unicodedata.normalize("NFKD", title).encode("ascii", "ignore").decode("ascii")
        title = title.lower()
        title = re.sub(r"[\s_\-]+", sep, title) 
        title = re.sub(rf"[^{re.escape(sep)}a-z0-9]", "", title)
        title = re.sub(rf"{re.escape(sep)}{{2,}}", sep, title)
        return title.strip(sep)

    @abstractmethod
    def _fetch_page(self, url: str) -> Optional[str]:
        pass

    @abstractmethod
    def _parse_content(self, html_content: str):
        pass

    @abstractmethod
    def _fetch_and_validate(self, formatted_title: str, year: int) -> Optional[Dict[str, int | float | None]]:
        pass


class HtmlScraper(BaseScraper):
    """
    Scraper for static HTML pages using requests.
    """
    REQUEST_DELAY_SECONDS = 0.5

    def __init__(self, base_url: str, robots_txt_path: str = "robots.txt", user_agent: str = ""):
        super().__init__(base_url, robots_txt_path, user_agent)
        self.session = requests.Session()
        self.session.headers.update({"User-Agent": self.user_agent})

    def _fetch_page(self, url: str) -> Optional[str]:
        try:
            logger.info(f"Fetching: {url}")
            response = self.session.get(url)
            response.raise_for_status()
            time.sleep(self.REQUEST_DELAY_SECONDS)
            return response.text
        except requests.exceptions.RequestException as e:
            logger.error(f"Error fetching {url}: {e}")
            return None


class PlaywrightScraper(BaseScraper):
    """
    Scraper for dynamic pages using Playwright.
    Handles JavaScript-rendered content and interactive elements.
    """
    
    def __init__(self, base_url: str, robots_txt_path: str = "robots.txt", user_agent: str = "", headless: bool = True):
        super().__init__(base_url, robots_txt_path, user_agent)
        self.playwright = None
        self.browser = None
        self.context = None
        self.page = None
        self.headless = headless
        self._setup_playwright()
    
    def _setup_playwright(self):
        """Initialize Playwright browser and context."""
        try:
            self.playwright = sync_playwright().start()
            self.browser = self.playwright.chromium.launch(
                headless=self.headless,  # Use the headless parameter
                args=[
                    '--no-sandbox',
                    '--disable-dev-shm-usage',
                    '--disable-gpu',
                    '--no-first-run',
                    '--no-zygote',
                    '--single-process'
                ]
            )
            self.context = self.browser.new_context(
                user_agent=self.user_agent,
                viewport={'width': 1920, 'height': 1080}
            )
            self.page = self.context.new_page()
            if not self.headless:
                logger.info("Playwright browser initialized in VISUAL mode (headless=False)")
            else:
                logger.info("Playwright browser initialized in headless mode")
        except Exception as e:
            logger.error(f"Failed to initialize Playwright: {e}")
            raise
    
    def _fetch_page(self, formatted_title: str, year: int) -> Optional[str]:
        """
        Fetch page content using Playwright with fallback to search functionality.
        Returns HTML content of the media-scorecard if found.
        """
        try:
            if not self.page:
                logger.error("Playwright page not initialized")
                return None
            
            # Try multiple URL patterns for the movie
            url_patterns = [
                f"m/{formatted_title}/",
                f"m/{formatted_title}_{year}/"
            ]
            
            for pattern in url_patterns:
                direct_url = urljoin(self.base_url, pattern)
                logger.info(f"Trying URL pattern: {direct_url}")
                
                try:
                    self.page.goto(direct_url, wait_until='domcontentloaded', timeout=5000)
                    
                    # Check if media-scorecard exists
                    media_scorecard = self.page.query_selector('media-scorecard')
                    if media_scorecard:
                        logger.info("Media scorecard found on direct page")
                        
                        # Check if ratings are incomplete (empty scores)
                        critics_score = self.page.query_selector('rt-text[slot="criticsScore"][context="label"]').inner_text().strip()
                        audience_score = self.page.query_selector('rt-text[slot="audienceScore"][context="label"]').inner_text().strip()
                        
                        if not critics_score:
                            logger.info("Critic score is empty, trying next URL pattern...")
                            continue
                        if not audience_score:
                            logger.info("Audience score is empty, trying next URL pattern...")
                            continue
                        
                        # Check year from metadata
                        year_match = self._check_year_match(year)
                        if year_match:
                            logger.info(f"Year match confirmed: {year}")
                            return self.page.content()
                        else:
                            logger.info(f"Year mismatch on direct page, trying next URL pattern...")
                            continue
                    else:
                        logger.info("Media scorecard not found on direct page, trying next URL pattern...")
                        continue
                        
                except Exception as e:
                    logger.info(f"Direct navigation failed for {pattern}: {e}, trying next pattern...")
                    continue
            
            # If all direct patterns fail, try search functionality
            logger.info("All direct URL patterns failed, trying search...")
            return self._search_and_extract(formatted_title, year)
            
        except Exception as e:
            logger.error(f"Error in _fetch_page: {e}")
            return None
    
    def _check_year_match(self, target_year: int) -> bool:
        """Check if the page year matches the target year within 3 years tolerance."""
        try:
            # Look for year in metadata props
            metadata_elements = self.page.query_selector_all('rt-text[slot="metadataProp"]')
            for element in metadata_elements:
                text = element.inner_text()
                if text:
                    year_match = re.search(r'(19|20)\d{2}', text)
                    if year_match:
                        page_year = int(year_match.group(0))
                        if abs(page_year - target_year) <= 3:
                            logger.info(f"Year match found: {page_year} (target: {target_year})")
                            return True
            
            return False
        except Exception as e:
            logger.error(f"Error checking year match: {e}")
            return False
    
    def _search_and_extract(self, formatted_title: str, year: int) -> Optional[str]:
        """Use search functionality to find the movie and extract content."""
        try:
            logger.info("Using search functionality to find movie")
            
            # Find and fill search input
            search_input = self.page.query_selector('[data-qa="search-input"]')
            if not search_input:
                logger.error("Search input not found")
                return None
            
            # Clear and type the search query
            search_input.fill("")
            search_input.type(formatted_title.replace('_', ' '))
            search_input.press('Enter')
            
            # Wait for search results
            self.page.wait_for_selector('[data-qa="search-results"]', timeout=5000)
            
            # Get first search result
            first_result = self.page.query_selector('[data-qa="data-row"]')
            if not first_result:
                logger.error("No search results found")
                return None
            
            # Check year in search result
            year_element = first_result.query_selector('[data-qa="info-year"]')
            if year_element:
                year_text = year_element.inner_text()
                year_match = re.search(r'\((\d{4})\)', year_text)
                if year_match:
                    result_year = int(year_match.group(1))
                    if abs(result_year - year) <= 3:
                        logger.info(f"Year match in search results: {result_year}")
                        # Click on the first result to navigate to its page
                        first_result = first_result.query_selector('[data-qa="info-name"]')
                        first_result.click()
                        
                        # Wait for page to load completely
                        self.page.wait_for_load_state('domcontentloaded', timeout=10000)

                        media_scorecard = self.page.query_selector('media-scorecard')
                        if media_scorecard:
                            logger.info(f"Media scorecard found after search navigation")
                            
                            return self.page.content()
                        
                        logger.warning("Media scorecard not found after search navigation")
                    else:
                        logger.info(f"Year mismatch in search results: {result_year} vs {year}")
                else:
                    logger.warning("Could not extract year from search result")
            
            return None
            
        except Exception as e:
            logger.error(f"Error in search and extract: {e}")
            return None
    
    def _parse_content(self, html_content: str):
        """Parse HTML content - to be implemented by subclasses."""
        raise NotImplementedError("Subclasses must implement _parse_content")
    
    def _fetch_and_validate(self, formatted_title: str, year: int) -> Optional[Dict[str, int | float | None]]:
        """Fetch and validate content - to be implemented by subclasses."""
        raise NotImplementedError("Subclasses must implement _fetch_and_validate")
    
    def close(self):
        """Explicitly close Playwright resources."""
        try:
            if hasattr(self, 'page') and self.page:
                self.page.close()
            if hasattr(self, 'context') and self.context:
                self.context.close()
            if hasattr(self, 'browser') and self.browser:
                self.browser.close()
            if hasattr(self, 'playwright') and self.playwright:
                self.playwright.stop()
        except Exception as e:
            logger.warning(f"Error during Playwright cleanup: {e}")
    
    def __del__(self):
        """Cleanup Playwright resources."""
        self.close()
    

