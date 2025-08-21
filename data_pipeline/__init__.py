__version__ = "1.0.0"
__author__ = "Mostafa Emad"

from .ingestor import SimpleIngestor
from .comprehensive_ingestor import ComprehensiveIngestor
from .db_inserter import DatabaseInserter
from .multi_table_inserter import MultiTableInserter

from .config import PipelineConfig, config
from .utils import (
    setup_logging, retry_on_error, safe_json_dump, safe_json_load,
    format_timestamp, parse_rating, parse_votes, parse_metascore,
    extract_year_from_date, truncate_text, clean_filename,
    get_file_size_mb, create_backup, validate_movie_data,
    calculate_processing_time, print_progress_bar,
    merge_movie_data, filter_movies_by_rating, sort_movies_by_rating
)

from .scrappers.base_scraper import BaseScraper, HtmlScraper, PlaywrightScraper
from .scrappers.metacritic_scraper import MetacriticScraper
from .scrappers.tomatos_scraper import RottenTomatoesScraper

__all__ = [
    # Core components
    'SimpleIngestor',
    'ComprehensiveIngestor', 
    'DatabaseInserter',
    'MultiTableInserter',
    
    # Configuration
    'PipelineConfig',
    'config',
    
    # Utilities
    'setup_logging',
    'retry_on_error',
    'safe_json_dump',
    'safe_json_load',
    'format_timestamp',
    'parse_rating',
    'parse_votes',
    'parse_metascore',
    'extract_year_from_date',
    'truncate_text',
    'clean_filename',
    'get_file_size_mb',
    'create_backup',
    'validate_movie_data',
    'calculate_processing_time',
    'print_progress_bar',
    'merge_movie_data',
    'filter_movies_by_rating',
    'sort_movies_by_rating',
    
    # Scrapers
    'BaseScraper',
    'HtmlScraper',
    'PlaywrightScraper',
    'MetacriticScraper',
    'RottenTomatoesScraper'
]
