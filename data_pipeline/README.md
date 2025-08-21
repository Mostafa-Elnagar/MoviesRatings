# Movie Ratings Data Pipeline

A comprehensive data pipeline for ingesting, enhancing, and storing movie data from multiple sources including TMDB, OMDb API, Metacritic, and Rotten Tomatoes.

## 🏗️ Architecture

The pipeline is built with a modular architecture that separates concerns:

- **Ingestion Layer**: Handles data collection from various sources
- **Enhancement Layer**: Enriches data with additional APIs and web scraping
- **Storage Layer**: Manages data persistence in Apache Iceberg via Trino
- **Configuration Layer**: Centralized settings and environment management

## 🚀 Quick Start

### Prerequisites

- Python 3.8+
- Docker with Docker Compose
- OMDb API key (set in `.env` file)

### Installation

1. Clone the repository
2. Install dependencies: `pip install -r requirements.txt`
3. Set up environment variables in `.env`
4. Start infrastructure: `docker-compose up -d`

### Basic Usage

```python
from data_pipeline import ComprehensiveIngestor

# Initialize ingestor
ingestor = ComprehensiveIngestor()

# Run comprehensive ingestion
results = ingestor.run_comprehensive_ingestion(max_movies=100)
```

## 📊 Pipeline Types

### 1. Simple Ingestion (`SimpleIngestor`)
- **Purpose**: Basic TMDB + OMDb API integration
- **Use Case**: Quick data collection without web scraping
- **Performance**: Fast, reliable, API-based only

```python
from data_pipeline import SimpleIngestor

ingestor = SimpleIngestor()
results = ingestor.run_ingestion(max_movies=50)
```

### 2. Comprehensive Ingestion (`ComprehensiveIngestor`)
- **Purpose**: Full multi-source data integration
- **Use Case**: Complete data enrichment with all sources
- **Performance**: Slower but more comprehensive

```python
from data_pipeline import ComprehensiveIngestor

ingestor = ComprehensiveIngestor()
results = ingestor.run_comprehensive_ingestion(max_movies=100)
```

## 🔧 Configuration

### Environment Variables

Create a `.env` file in the project root:

```bash
# API Keys
OMDB_API_KEY=your_omdb_api_key_here

# Database Configuration
TRINO_HOST=localhost
TRINO_PORT=8080
TRINO_CATALOG=iceberg
TRINO_SCHEMA=movies_stage

# Rate Limiting
OMDB_RATE_LIMIT=0.1
SCRAPER_RATE_LIMIT=1.0
```

### Pipeline Configuration

```python
from data_pipeline.config import PipelineConfig

# Load configuration
config = PipelineConfig()

# Validate configuration
if config.validate_config():
    print("✅ Configuration is valid")
```

## 📁 Data Flow

```
Raw TMDB Data → Enhancement → Storage
     ↓              ↓          ↓
  data/raw/    data/processed/  Trino/Iceberg
```

### Data Sources

1. **TMDB**: Base movie metadata
2. **OMDb API**: Additional movie information
3. **Metacritic**: Critic and user ratings
4. **Rotten Tomatoes**: Audience and critic scores

### Output Structure

- **Raw Data**: `data/raw/` - Original TMDB JSON files
- **Processed Data**: `data/processed/` - Enhanced movie data
- **Database**: Trino/Iceberg tables for analysis

## 🗄️ Database Schema

The pipeline creates and populates multiple tables:

### `tmdb_movies`
- Core TMDB movie data
- Includes metadata, ratings, and production info

### `omdb_movies`
- OMDb API enriched data
- Additional movie details and ratings

### `metacritic_ratings`
- Metacritic critic and user scores
- Rating counts and metadata

### `rotten_tomatoes_ratings`
- Rotten Tomatoes audience and critic scores
- Rating counts and metadata

## 🛠️ Customization

### Adding New Data Sources

1. Create a new scraper class inheriting from `BaseScraper`
2. Implement required methods: `_fetch_page`, `_parse_content`, `_fetch_and_validate`
3. Add transformation logic in `MultiTableInserter`
4. Update the comprehensive ingestor

### Modifying Data Transformations

Edit the transformation methods in `MultiTableInserter`:

```python
def transform_for_custom_table(self, movie: Dict[str, Any]) -> Dict[str, Any]:
    # Custom transformation logic
    return transformed_movie
```

## 📈 Monitoring & Logging

### Logging Configuration

The pipeline uses structured logging with different levels:

```python
import logging

# Configure logging level
logging.basicConfig(level=logging.INFO)

# Pipeline logs include:
# - Data processing progress
# - API call results
# - Database operation status
# - Error details and stack traces
```

### Progress Tracking

The pipeline provides real-time progress updates:

```
🎬 Processing: tmdb_top_rated_movies_20250821_002654.json
   📊 Processing 100 movies...
   🎥   1/100: The Shawshank Redemption
   🎥   2/100: The Godfather
      ✅ Processed 10/100 movies
```

## 🚨 Error Handling

### Graceful Degradation

- **API Failures**: Continue processing with available data
- **Scraper Errors**: Fall back to API-only enhancement
- **Database Issues**: Retry with exponential backoff

### Error Recovery

```python
try:
    results = ingestor.run_comprehensive_ingestion()
except Exception as e:
    logger.error(f"Pipeline failed: {e}")
    # Handle error gracefully
```

## 🔒 Security Considerations

### API Key Management

- Store API keys in environment variables
- Never commit `.env` files to version control
- Use `.env.example` for documentation

### Rate Limiting

- Respect API rate limits (OMDb: 0.1s, Scrapers: 1.0s)
- Implement exponential backoff for failures
- Monitor API usage to avoid hitting limits

## 📦 Dependencies

### Core Dependencies

- `requests`: HTTP client for API calls
- `playwright`: Web scraping for dynamic content
- `beautifulsoup4`: HTML parsing
- `pandas`: Data manipulation (if needed)

### Infrastructure

- `docker`: Containerization
- `trino`: SQL query engine
- `apache-iceberg`: Table format
- `minio`: S3-compatible storage

## 🤝 Contributing

### Development Setup

1. Fork the repository
2. Create a feature branch
3. Make changes with tests
4. Submit a pull request

### Code Style

- Follow PEP 8 guidelines
- Use type hints
- Document all public methods
- Include error handling

## 📄 License

This project is licensed under the MIT License - see the LICENSE file for details.

## 🆘 Support

For issues and questions:

1. Check the documentation
2. Review existing issues
3. Create a new issue with details
4. Include error logs and configuration
