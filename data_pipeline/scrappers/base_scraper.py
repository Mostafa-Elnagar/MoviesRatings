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

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
logger = logging.getLogger("scraper")

class BaseScraper(ABC):
    """Base class for all scrapers with common functionality."""
    
    DEFAULT_USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"

    def __init__(self, base_url: str, robots_txt_path: str = "robots.txt", user_agent: str = ""):
        if not base_url.endswith('/'):
            base_url += '/'
        self.base_url = base_url
        self.robots_txt_url = urljoin(self.base_url, robots_txt_path or "robots.txt")
        self.user_agent = user_agent or self.DEFAULT_USER_AGENT
        self.robot_parser = urllib.robotparser.RobotFileParser()
        self._load_robots_txt()

    def _load_robots_txt(self) -> None:
        """Load and parse robots.txt file."""
        try:
            self.robot_parser.set_url(self.robots_txt_url)
            self.robot_parser.read()
            logger.info(f"Loaded robots.txt from {self.robots_txt_url}")
        except Exception as e:
            logger.warning(f"Failed to load robots.txt: {e}")

    def is_scraping_allowed(self, url: str) -> bool:
        """Check if scraping is allowed for the given URL."""
        parsed_url = urlparse(url)
        return self.robot_parser.can_fetch(self.user_agent, parsed_url.path)
    
    def get_ratings(self, movie_title: str, year: int, sep: str) -> Optional[Dict[str, int | float | None]]:
        """Get ratings for a movie with title preprocessing."""
        if not movie_title:
            logger.error("Movie title cannot be empty.")
            return {}
        
        formatted_title = self._preprocess_title(movie_title, sep=sep)
        result = self._fetch_and_validate(formatted_title, year)
        
        if result:
            result['title'] = movie_title
            result['year'] = year
        
        return result
    
    def _preprocess_title(self, title: str, sep: str) -> str:
        """Convert title to URL-safe slug."""
        title = unicodedata.normalize("NFKD", title).encode("ascii", "ignore").decode("ascii")
        title = title.lower()
        title = re.sub(r"[\s_\-]+", sep, title) 
        title = re.sub(rf"[^{re.escape(sep)}a-z0-9]", "", title)
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
    """Simple scraper for static HTML pages using requests."""
    
    REQUEST_DELAY = 0.5

    def __init__(self, base_url: str, robots_txt_path: str = "robots.txt", user_agent: str = ""):
        super().__init__(base_url, robots_txt_path, user_agent)
        self.session = requests.Session()
        self.session.headers.update({"User-Agent": self.user_agent})

    def _fetch_page(self, url: str) -> Optional[str]:
        """Fetch page content with rate limiting."""
        try:
            logger.info(f"Fetching: {url}")
            response = self.session.get(url)
            response.raise_for_status()
            time.sleep(self.REQUEST_DELAY)
            return response.text
        except requests.exceptions.RequestException as e:
            logger.error(f"Error fetching {url}: {e}")
            return None


class PlaywrightScraper(BaseScraper):
    """Scraper for dynamic pages using Playwright."""
    
    def __init__(self, base_url: str, robots_txt_path: str = "robots.txt", user_agent: str = "", headless: bool = True):
        super().__init__(base_url, robots_txt_path, user_agent)
        self.headless = headless
        self.playwright = None
        self.browser = None
        self.context = None
        self.page = None
        self._setup_playwright()
    
    def _setup_playwright(self):
        """Initialize Playwright browser and context."""
        try:
            self.playwright = sync_playwright().start()
            self.browser = self.playwright.chromium.launch(
                headless=self.headless,
                args=['--no-sandbox', '--disable-dev-shm-usage', '--disable-gpu']
            )
            self.context = self.browser.new_context(user_agent=self.user_agent)
            self.page = self.context.new_page()
            logger.info(f"Playwright initialized in {'headless' if self.headless else 'visual'} mode")
        except Exception as e:
            logger.error(f"Failed to initialize Playwright: {e}")
            raise
    
    def _fetch_page(self, formatted_title: str, year: int) -> Optional[str]:
        """Fetch page content with multiple URL patterns and search fallback."""
        try:
            if not self.page:
                logger.error("Playwright page not initialized")
                return None
            
            # Try direct URL patterns first
            url_patterns = [
                f"m/{formatted_title}/",
                f"m/{formatted_title}_{year}/"
            ]
            
            for pattern in url_patterns:
                direct_url = urljoin(self.base_url, pattern)
                logger.info(f"Trying: {direct_url}")
                
                try:
                    self.page.goto(direct_url, wait_until='domcontentloaded', timeout=5000)
                    
                    if self._has_media_scorecard():
                        logger.info("Media scorecard found on direct page")
                        if self._check_year_match(year):
                            return self.page.content()
                    
                except Exception as e:
                    logger.info(f"Direct navigation failed: {e}")
                    continue
            
            # Fallback to search
            logger.info("Trying search functionality")
            return self._search_and_extract(formatted_title, year)
            
        except Exception as e:
            logger.error(f"Error in _fetch_page: {e}")
            return None
    
    def _has_media_scorecard(self) -> bool:
        """Check if media scorecard exists on the page."""
        try:
            scorecard = self.page.query_selector('media-scorecard')
            if scorecard:
                # Check if scores are present
                critic_score = self.page.query_selector('rt-text[slot="criticsScore"]')
                audience_score = self.page.query_selector('rt-text[slot="audienceScore"]')
                return critic_score and audience_score
            return False
        except Exception:
            return False
    
    def _check_year_match(self, target_year: int) -> bool:
        """Check if page year matches target year within tolerance."""
        try:
            metadata_elements = self.page.query_selector_all('rt-text[slot="metadataProp"]')
            for element in metadata_elements:
                text = element.inner_text()
                if text:
                    year_match = re.search(r'(19|20)\d{2}', text)
                    if year_match:
                        page_year = int(year_match.group(0))
                        if abs(page_year - target_year) <= 3:
                            logger.info(f"Year match: {page_year} (target: {target_year})")
                            return True
            return False
        except Exception as e:
            logger.error(f"Error checking year match: {e}")
            return False
    
    def _search_and_extract(self, formatted_title: str, year: int) -> Optional[str]:
        """Use search to find movie and extract content."""
        try:
            logger.info("Using search to find movie")
            
            # Find search input and search
            search_input = self.page.query_selector('[data-qa="search-input"]')
            if not search_input:
                logger.error("Search input not found")
                return None
            
            search_input.fill("")
            search_input.type(formatted_title.replace('_', ' '))
            search_input.press('Enter')
            
            # Wait for results and get first one
            self.page.wait_for_selector('[data-qa="search-results"]', timeout=5000)
            first_result = self.page.query_selector('[data-qa="data-row"]')
            if not first_result:
                logger.error("No search results found")
                return None
            
            # Check year match in search result
            year_element = first_result.query_selector('[data-qa="info-year"]')
            if year_element:
                year_text = year_element.inner_text()
                year_match = re.search(r'\((\d{4})\)', year_text)
                if year_match:
                    result_year = int(year_match.group(1))
                    if abs(result_year - year) <= 3:
                        logger.info(f"Year match in search: {result_year}")
                        
                        # Click on result and wait for page load
                        name_element = first_result.query_selector('[data-qa="info-name"]')
                        if name_element:
                            name_element.click()
                            self.page.wait_for_load_state('domcontentloaded', timeout=10000)
                            
                            if self._has_media_scorecard():
                                logger.info("Media scorecard found after search")
                                return self.page.content()
            
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
        """Clean up Playwright resources."""
        try:
            if self.page:
                self.page.close()
            if self.context:
                self.context.close()
            if self.browser:
                self.browser.close()
            if self.playwright:
                self.playwright.stop()
        except Exception as e:
            logger.warning(f"Error during cleanup: {e}")
    
    def __del__(self):
        """Automatic cleanup."""
        self.close()
    

