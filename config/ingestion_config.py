"""
Configuration for the comprehensive movie data ingestion pipeline
"""

import os
from pathlib import Path

# Data directories
RAW_DATA_DIR = Path("data/raw")
PROCESSED_DATA_DIR = Path("data/processed")
ANALYSIS_DATA_DIR = Path("data/analysis")
LOGS_DIR = Path("logs")

# API Configuration
OMDB_API_KEY = os.getenv('OMDB_API_KEY')
OMDB_BASE_URL = "http://www.omdbapi.com/"
OMDB_TIMEOUT = 10  # seconds

# Rate Limiting (in seconds)
OMDB_DELAY = 0.1  # 100ms between OMDb API calls
SCRAPER_DELAY = 1.0  # 1 second between scraping calls

# Scraping Configuration
METACRITIC_ENABLED = True
ROTTEN_TOMATOES_ENABLED = True
SCRAPER_HEADLESS = True  # Run scrapers in headless mode

# Data Processing
BATCH_SIZE = 100  # Process movies in batches
PROGRESS_UPDATE_INTERVAL = 10  # Show progress every N movies
MAX_RETRIES = 3  # Maximum retries for failed requests

# Output Configuration
SAVE_INTERMEDIATE_RESULTS = True  # Save results after each file
COMPRESS_OUTPUT = False  # Whether to compress output files
OUTPUT_FORMAT = "json"  # Output format (json, parquet, csv)

# Logging Configuration
LOG_LEVEL = "INFO"
LOG_FORMAT = "%(asctime)s %(levelname)s %(message)s"
LOG_FILE = "logs/ingestion.log"
LOG_MAX_SIZE = 10 * 1024 * 1024  # 10MB
LOG_BACKUP_COUNT = 5

# Error Handling
CONTINUE_ON_ERROR = True  # Continue processing if individual movies fail
MAX_ERRORS = 100  # Maximum number of errors before stopping
ERROR_LOG_FILE = "logs/ingestion_errors.log"

# Data Validation
VALIDATE_YEAR_MATCH = True  # Validate year matches between sources
YEAR_TOLERANCE = 3  # Years tolerance for matching
MIN_RATING_THRESHOLD = 0  # Minimum rating threshold to consider valid

# TMDB Data Processing
TMDB_FIELD_MAPPING = {
    'id': 'tmdb_id',
    'title': 'title',
    'original_title': 'original_title',
    'release_date': 'release_date',
    'overview': 'overview',
    'tagline': 'tagline',
    'status': 'status',
    'runtime': 'runtime',
    'budget': 'budget',
    'revenue': 'revenue',
    'popularity': 'popularity',
    'vote_average': 'vote_average',
    'vote_count': 'vote_count',
    'genres': 'genres',
    'genre_ids': 'genre_ids',
    'original_language': 'original_language',
    'production_companies': 'production_companies',
    'production_countries': 'production_countries',
    'spoken_languages': 'spoken_languages',
    'cast_data': 'cast_data',
    'crew_data': 'crew_data',
    'backdrop_path': 'backdrop_path',
    'poster_path': 'poster_path',
    'homepage': 'homepage',
    'external_ids': 'external_ids'
}

# OMDb Data Mapping
OMDB_FIELD_MAPPING = {
    'Title': 'omdb_title',
    'Rated': 'omdb_rated',
    'Released': 'omdb_released',
    'Runtime': 'omdb_runtime',
    'Genre': 'omdb_genre',
    'Director': 'omdb_director',
    'Writer': 'omdb_writer',
    'Actors': 'omdb_actors',
    'Plot': 'omdb_plot',
    'Language': 'omdb_language',
    'Country': 'omdb_country',
    'Awards': 'omdb_awards',
    'Poster': 'omdb_poster',
    'Ratings': 'omdb_ratings',
    'imdbRating': 'omdb_imdb_rating',
    'imdbVotes': 'omdb_imdb_votes',
    'Metascore': 'omdb_metascore',
    'BoxOffice': 'omdb_box_office',
    'Production': 'omdb_production',
    'Website': 'omdb_website'
}

# Metacritic Data Mapping
METACRITIC_FIELD_MAPPING = {
    'critic_score': 'metacritic_critic_score',
    'critic_count': 'metacritic_critic_count',
    'user_score': 'metacritic_user_score',
    'user_count': 'metacritic_user_count'
}

# Rotten Tomatoes Data Mapping
ROTTEN_TOMATOES_FIELD_MAPPING = {
    'critic_score': 'rt_critic_score',
    'critic_count': 'rt_critic_count',
    'user_score': 'rt_user_score',
    'user_count': 'rt_user_count'
}

# Output File Naming
OUTPUT_FILE_PATTERN = "enhanced_{source}_{timestamp}.{extension}"
ANALYSIS_FILE_PATTERN = "comprehensive_movies_{timestamp}.{extension}"

# Performance Configuration
USE_MULTIPROCESSING = False  # Enable multiprocessing for large datasets
MAX_WORKERS = 4  # Maximum number of worker processes
CHUNK_SIZE = 50  # Number of movies per chunk for multiprocessing

# Monitoring and Metrics
ENABLE_METRICS = True  # Collect performance metrics
METRICS_INTERVAL = 60  # Metrics collection interval in seconds
SAVE_METRICS = True  # Save metrics to file
METRICS_FILE = "logs/ingestion_metrics.json"
