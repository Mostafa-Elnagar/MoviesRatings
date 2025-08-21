#!/usr/bin/env python3
"""
Utility functions for the Movie Ratings Data Pipeline
Common helper functions and utilities
"""

import json
import logging
import time
from pathlib import Path
from typing import Any, Dict, List, Optional
from datetime import datetime
from functools import wraps

def setup_logging(name: str, level: str = 'INFO', log_file: Optional[str] = None) -> logging.Logger:
    """Set up logging for a module"""
    logger = logging.getLogger(name)
    logger.setLevel(getattr(logging, level.upper()))
    
    # Create formatter
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    # Console handler
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)
    
    # File handler (if specified)
    if log_file:
        file_handler = logging.FileHandler(log_file)
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)
    
    return logger

def retry_on_error(max_retries: int = 3, delay: float = 1.0, backoff: float = 2.0):
    """Decorator to retry functions on error"""
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            last_exception = None
            
            for attempt in range(max_retries):
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    last_exception = e
                    if attempt < max_retries - 1:
                        sleep_time = delay * (backoff ** attempt)
                        time.sleep(sleep_time)
                        continue
                    else:
                        raise last_exception
            
            return None
        return wrapper
    return decorator

def safe_json_dump(data: Any, file_path: Path, **kwargs) -> bool:
    """Safely dump JSON data to file with error handling"""
    try:
        # Ensure directory exists
        file_path.parent.mkdir(parents=True, exist_ok=True)
        
        # Write to temporary file first
        temp_path = file_path.with_suffix('.tmp')
        with open(temp_path, 'w', encoding='utf-8') as f:
            json.dump(data, f, **kwargs)
        
        # Move to final location
        temp_path.replace(file_path)
        return True
        
    except Exception as e:
        print(f"❌ Error saving JSON to {file_path}: {e}")
        return False

def safe_json_load(file_path: Path) -> Optional[Any]:
    """Safely load JSON data from file with error handling"""
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            return json.load(f)
    except Exception as e:
        print(f"❌ Error loading JSON from {file_path}: {e}")
        return None

def format_timestamp(timestamp: Optional[datetime] = None, format_str: str = '%Y%m%d_%H%M%S') -> str:
    """Format timestamp to string"""
    if timestamp is None:
        timestamp = datetime.now()
    return timestamp.strftime(format_str)

def parse_rating(rating_str: Optional[str]) -> Optional[float]:
    """Parse rating string to float"""
    if not rating_str or rating_str == 'N/A':
        return None
    try:
        return float(rating_str)
    except ValueError:
        return None

def parse_votes(votes_str: Optional[str]) -> Optional[int]:
    """Parse votes string to integer"""
    if not votes_str or votes_str == 'N/A':
        return None
    try:
        return int(votes_str.replace(',', ''))
    except ValueError:
        return None

def parse_metascore(metascore_str: Optional[str]) -> Optional[int]:
    """Parse metascore string to integer"""
    if not metascore_str or metascore_str == 'N/A':
        return None
    try:
        return int(metascore_str)
    except ValueError:
        return None

def extract_year_from_date(date_str: Optional[str]) -> Optional[int]:
    """Extract year from date string"""
    if not date_str:
        return None
    try:
        return int(date_str[:4])
    except (ValueError, TypeError):
        return None

def truncate_text(text: str, max_length: int = 1000, suffix: str = "...") -> str:
    """Truncate text to specified length"""
    if len(text) <= max_length:
        return text
    return text[:max_length - len(suffix)] + suffix

def clean_filename(filename: str) -> str:
    """Clean filename for safe file system usage"""
    # Remove or replace problematic characters
    invalid_chars = '<>:"/\\|?*'
    for char in invalid_chars:
        filename = filename.replace(char, '_')
    
    # Remove leading/trailing spaces and dots
    filename = filename.strip(' .')
    
    # Limit length
    if len(filename) > 200:
        filename = filename[:200]
    
    return filename

def get_file_size_mb(file_path: Path) -> float:
    """Get file size in megabytes"""
    try:
        size_bytes = file_path.stat().st_size
        return size_bytes / (1024 * 1024)
    except Exception:
        return 0.0

def create_backup(file_path: Path, backup_dir: Path = Path("backups")) -> Optional[Path]:
    """Create a backup of a file"""
    try:
        backup_dir.mkdir(parents=True, exist_ok=True)
        
        timestamp = format_timestamp()
        backup_name = f"{file_path.stem}_{timestamp}{file_path.suffix}"
        backup_path = backup_dir / backup_name
        
        # Copy file
        import shutil
        shutil.copy2(file_path, backup_path)
        
        return backup_path
        
    except Exception as e:
        print(f"❌ Error creating backup of {file_path}: {e}")
        return None

def validate_movie_data(movie: Dict[str, Any]) -> List[str]:
    """Validate movie data and return list of validation errors"""
    errors = []
    
    # Required fields
    required_fields = ['title', 'imdb_id']
    for field in required_fields:
        if not movie.get(field):
            errors.append(f"Missing required field: {field}")
    
    # Validate rating range
    rating = movie.get('omdb_imdb_rating')
    if rating is not None:
        if not isinstance(rating, (int, float)) or rating < 0 or rating > 10:
            errors.append(f"Invalid rating value: {rating}")
    
    # Validate year range
    year = movie.get('year')
    if year is not None:
        if not isinstance(year, int) or year < 1900 or year > 2030:
            errors.append(f"Invalid year value: {year}")
    
    return errors

def calculate_processing_time(start_time: float) -> str:
    """Calculate and format processing time"""
    elapsed = time.time() - start_time
    
    if elapsed < 60:
        return f"{elapsed:.1f} seconds"
    elif elapsed < 3600:
        minutes = elapsed / 60
        return f"{minutes:.1f} minutes"
    else:
        hours = elapsed / 3600
        return f"{hours:.1f} hours"

def print_progress_bar(current: int, total: int, width: int = 50, title: str = "Progress"):
    """Print a progress bar"""
    if total == 0:
        return
    
    percentage = current / total
    filled_width = int(width * percentage)
    bar = '█' * filled_width + '░' * (width - filled_width)
    
    print(f"\r{title}: [{bar}] {current}/{total} ({percentage:.1%})", end='', flush=True)
    
    if current == total:
        print()  # New line when complete

def merge_movie_data(base_movie: Dict[str, Any], enhancement: Dict[str, Any]) -> Dict[str, Any]:
    """Merge enhancement data into base movie data"""
    merged = base_movie.copy()
    
    for key, value in enhancement.items():
        if value is not None:  # Only overwrite with non-None values
            merged[key] = value
    
    return merged

def filter_movies_by_rating(movies: List[Dict[str, Any]], min_rating: float = 0.0, 
                           max_rating: float = 10.0) -> List[Dict[str, Any]]:
    """Filter movies by rating range"""
    filtered = []
    
    for movie in movies:
        rating = movie.get('omdb_imdb_rating')
        if rating is not None and min_rating <= rating <= max_rating:
            filtered.append(movie)
    
    return filtered

def sort_movies_by_rating(movies: List[Dict[str, Any]], reverse: bool = True) -> List[Dict[str, Any]]:
    """Sort movies by rating"""
    return sorted(movies, key=lambda x: x.get('omdb_imdb_rating', 0), reverse=reverse)
