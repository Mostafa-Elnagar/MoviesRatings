# 🎬 Top-Rated Movies Pipeline

A comprehensive data ingestion pipeline that fetches **ALL** top-rated movies from TMDB's top-rated endpoint, enriches them with OMDb data, and prepares them for analysis in your data lakehouse.

## 🚀 What This Pipeline Does

### **Phase 1: TMDB Data Collection**
- Fetches movies from TMDB's `/movie/top_rated` endpoint
- Processes **all 500+ pages** (10,000+ movies)
- Gets detailed movie information including:
  - Basic metadata (title, year, overview, etc.)
  - Cast and crew information
  - Genre classifications
  - Production details
  - External IDs (IMDb, etc.)
  - Ratings and vote counts

### **Phase 2: OMDb Enrichment**
- Enriches movies with IMDb IDs using OMDb API
- Adds additional metadata:
  - IMDb ratings and vote counts
  - Metascore ratings
  - Box office information
  - Awards and nominations
  - Production details

### **Phase 3: Data Staging**
- Saves all data to structured JSON files
- Organizes data by source and timestamp
- Prepares data for loading into Iceberg tables

## 📊 Current Data Status

**✅ Successfully Completed:**
- **TMDB Movies**: 1,000 top-rated movies fetched
- **OMDb Enrichment**: 100 movies enriched with additional data
- **Data Files**: Generated and staged in `data/raw/`

**📈 Data Quality:**
- **TMDB Ratings**: Average 7.90/10 (Range: 7.6-8.7)
- **IMDb Ratings**: Average 8.23/10 (Range: 5.9-9.3)
- **Metascore**: Average 81.7/100 (Range: 53-100)

## 🛠️ How to Use

### **Option 1: Quick Test (Recommended for first run)**
```bash
# Test with 50 pages (1,000 movies)
python data_pipeline/top_rated_ingestor.py
```

### **Option 2: Interactive Pipeline**
```bash
# Run with user configuration
python scripts/run_top_rated_pipeline.py
```

### **Option 3: Full Pipeline (All Pages)**
```bash
# Fetch ALL top-rated movies (500+ pages)
python scripts/run_full_top_rated_pipeline.py
```

### **Option 4: Analyze Current Data**
```bash
# Analyze the data you've already fetched
python scripts/test_top_rated_data.py
```

## ⚙️ Configuration

The pipeline uses a YAML configuration file at `config/top_rated_pipeline_config.yaml`:

```yaml
tmdb:
  max_pages: 500          # Maximum pages to fetch
  movies_per_page: 20     # Movies per page
  rate_limit_delay: 0.25  # Delay between requests

omdb:
  max_enrichments: 1000   # Maximum OMDb enrichments
  rate_limit_delay: 0.1   # Delay between requests

scraping:
  max_movies: 100         # Maximum scraping operations
```

## 📁 Output Files

After running the pipeline, you'll find these files in `data/raw/`:

- **`tmdb_top_rated_movies_YYYYMMDD_HHMMSS.json`** - TMDB movie data
- **`omdb_top_rated_movies_YYYYMMDD_HHMMSS.json`** - OMDb enriched data
- **`metacritic_top_rated_ratings_YYYYMMDD_HHMMSS.json`** - Metacritic ratings (if scraping enabled)
- **`rotten_tomatoes_top_rated_ratings_YYYYMMDD_HHMMSS.json`** - Rotten Tomatoes ratings (if scraping enabled)

## 🔄 Next Steps

### **1. Load Data into Data Lakehouse**
```bash
# Load the staged data into Iceberg tables
python data_pipeline/stage_data_loader.py
```

### **2. Run Analytical Queries**
```bash
# Connect to Trino and query your data
docker exec -it trino trino --server localhost:8080 --catalog iceberg --schema movies_stage
```

### **3. Sample Queries**
```sql
-- Top 10 movies by TMDB rating
SELECT title, year, vote_average, vote_count 
FROM tmdb_movies 
ORDER BY vote_average DESC 
LIMIT 10;

-- Movies by decade
SELECT 
  CASE 
    WHEN year < 1970 THEN '1960s'
    WHEN year < 1980 THEN '1970s'
    WHEN year < 1990 THEN '1980s'
    WHEN year < 2000 THEN '1990s'
    WHEN year < 2010 THEN '2000s'
    WHEN year < 2020 THEN '2010s'
    ELSE '2020s'
  END as decade,
  COUNT(*) as movie_count,
  AVG(vote_average) as avg_rating
FROM tmdb_movies 
GROUP BY decade 
ORDER BY decade;

-- Genre analysis
SELECT 
  unnest(genres) as genre,
  COUNT(*) as movie_count,
  AVG(vote_average) as avg_rating
FROM tmdb_movies 
GROUP BY genre 
ORDER BY movie_count DESC;
```

## ⚠️ Important Notes

### **Rate Limiting**
- **TMDB**: 40 requests per 10 seconds (4 req/sec)
- **OMDb**: 1,000 requests per day (free tier)
- **Scraping**: Respects robots.txt and implements delays

### **API Keys Required**
Make sure your `.env` file contains:
```bash
TMDB_API_KEY=your_tmdb_api_key_here
OMDB_API_KEY=your_omdb_api_key_here
```

### **Data Volume**
- **50 pages**: ~1,000 movies, ~10 minutes
- **500 pages**: ~10,000 movies, ~2-3 hours
- **All pages**: ~10,000+ movies, ~3-4 hours

## 🚨 Troubleshooting

### **Common Issues**

1. **"API key not found"**
   - Check your `.env` file
   - Ensure API keys are valid

2. **"Rate limit exceeded"**
   - Wait and retry
   - Reduce `max_pages` or increase delays

3. **"Scraping failed"**
   - Scraping is optional
   - Focus on API data first

4. **"Memory issues"**
   - Process in smaller batches
   - Use `max_pages` to limit data volume

### **Logs**
Check the logs in `logs/top_rated_ingestion.log` for detailed information.

## 🎯 Use Cases

### **Data Analysis**
- Movie rating trends over time
- Genre popularity analysis
- Cast and crew correlation with ratings
- Production company performance

### **Recommendation Systems**
- Build movie recommendation engines
- Analyze rating patterns
- Identify similar movies

### **Market Research**
- Box office performance analysis
- Audience preference trends
- Genre evolution over decades

## 🔮 Future Enhancements

- **Real-time Updates**: Schedule regular data refreshes
- **Additional Sources**: Integrate more rating platforms
- **Machine Learning**: Build predictive models
- **Dashboard**: Create visualization dashboards
- **API Endpoints**: Expose data via REST API

## 📞 Support

If you encounter issues:
1. Check the logs in `logs/`
2. Verify your API keys
3. Review the configuration
4. Test with smaller data volumes first

---

**🎉 Ready to fetch the world's top-rated movies? Start with the quick test and scale up from there!**
