#!/usr/bin/env python3
"""
Test Top-Rated Movies Data
Display and analyze the data fetched by the top-rated pipeline
"""

import json
import sys
from pathlib import Path

def load_latest_top_rated_data():
    """Load the latest top-rated movies data files"""
    data_dir = Path("data/raw")
    
    # Find latest TMDB top-rated file
    tmdb_files = list(data_dir.glob("tmdb_top_rated_movies_*.json"))
    if not tmdb_files:
        print("No TMDB top-rated movies files found!")
        return None, None
    
    latest_tmdb = max(tmdb_files, key=lambda x: x.stat().st_mtime)
    
    # Find latest OMDb top-rated file
    omdb_files = list(data_dir.glob("omdb_top_rated_movies_*.json"))
    latest_omdb = max(omdb_files, key=lambda x: x.stat().st_mtime) if omdb_files else None
    
    print(f"📁 Loading data from:")
    print(f"  TMDB: {latest_tmdb.name}")
    if latest_omdb:
        print(f"  OMDb: {latest_omdb.name}")
    
    # Load TMDB data
    with open(latest_tmdb, 'r', encoding='utf-8') as f:
        tmdb_data = json.load(f)
    
    # Load OMDb data
    omdb_data = None
    if latest_omdb:
        with open(latest_omdb, 'r', encoding='utf-8') as f:
            omdb_data = json.load(f)
    
    return tmdb_data, omdb_data

def analyze_tmdb_data(tmdb_data):
    """Analyze TMDB data"""
    if not tmdb_data:
        return
    
    print(f"\n🎬 TMDB Top-Rated Movies Analysis")
    print("=" * 50)
    print(f"Total movies: {len(tmdb_data):,}")
    
    # Year distribution
    years = {}
    for movie in tmdb_data:
        year = movie.get('year')
        if year:
            years[year] = years.get(year, 0) + 1
    
    print(f"\n📅 Year Distribution (Top 10):")
    for year in sorted(years.keys(), reverse=True)[:10]:
        print(f"  {year}: {years[year]} movies")
    
    # Genre analysis
    all_genres = []
    for movie in tmdb_data:
        genres = movie.get('genres', [])
        all_genres.extend(genres)
    
    genre_counts = {}
    for genre in all_genres:
        genre_counts[genre] = genre_counts.get(genre, 0) + 1
    
    print(f"\n🎭 Top Genres:")
    for genre, count in sorted(genre_counts.items(), key=lambda x: x[1], reverse=True)[:10]:
        print(f"  {genre}: {count} movies")
    
    # Rating analysis
    ratings = [movie.get('vote_average', 0) for movie in tmdb_data if movie.get('vote_average')]
    if ratings:
        avg_rating = sum(ratings) / len(ratings)
        min_rating = min(ratings)
        max_rating = max(ratings)
        print(f"\n⭐ Rating Analysis:")
        print(f"  Average: {avg_rating:.2f}")
        print(f"  Range: {min_rating:.1f} - {max_rating:.1f}")
    
    # Top 10 movies by rating
    print(f"\n🏆 Top 10 Movies by Rating:")
    top_movies = sorted(tmdb_data, key=lambda x: x.get('vote_average', 0), reverse=True)[:10]
    for i, movie in enumerate(top_movies, 1):
        title = movie.get('title', 'Unknown')
        year = movie.get('year', 'Unknown')
        rating = movie.get('vote_average', 0)
        votes = movie.get('vote_count', 0)
        print(f"  {i:2d}. {title} ({year}) - {rating:.1f} ⭐ ({votes:,} votes)")

def analyze_omdb_data(omdb_data):
    """Analyze OMDb data"""
    if not omdb_data:
        return
    
    print(f"\n🎯 OMDb Enriched Data Analysis")
    print("=" * 50)
    print(f"Total enriched movies: {len(omdb_data):,}")
    
    # Rating analysis
    imdb_ratings = [m.get('imdb_rating', 0) for m in omdb_data if m.get('imdb_rating')]
    if imdb_ratings:
        avg_imdb = sum(imdb_ratings) / len(imdb_ratings)
        print(f"\n📊 IMDb Ratings:")
        print(f"  Average: {avg_imdb:.2f}")
        print(f"  Range: {min(imdb_ratings):.1f} - {max(imdb_ratings):.1f}")
    
    # Metascore analysis
    metascores = [m.get('metascore', 0) for m in omdb_data if m.get('metascore')]
    if metascores:
        avg_metascore = sum(metascores) / len(metascores)
        print(f"\n📈 Metascore:")
        print(f"  Average: {avg_metascore:.1f}")
        print(f"  Range: {min(metascores)} - {max(metascores)}")
    
    # Top rated movies
    print(f"\n🏆 Top 10 Movies by IMDb Rating:")
    top_omdb = sorted(omdb_data, key=lambda x: x.get('imdb_rating', 0), reverse=True)[:10]
    for i, movie in enumerate(top_omdb, 1):
        title = movie.get('title', 'Unknown')
        year = movie.get('year', 'Unknown')
        imdb_rating = movie.get('imdb_rating', 0)
        metascore = movie.get('metascore', 'N/A')
        print(f"  {i:2d}. {title} ({year}) - IMDb: {imdb_rating:.1f} ⭐, Meta: {metascore}")

def main():
    """Main function"""
    print("🎬 Top-Rated Movies Data Analysis")
    print("=" * 60)
    
    # Load data
    tmdb_data, omdb_data = load_latest_top_rated_data()
    
    if not tmdb_data:
        print("❌ No data found. Run the top-rated pipeline first.")
        return False
    
    # Analyze data
    analyze_tmdb_data(tmdb_data)
    analyze_omdb_data(omdb_data)
    
    print(f"\n✅ Analysis complete!")
    print(f"📊 Summary:")
    print(f"  • TMDB Movies: {len(tmdb_data):,}")
    print(f"  • OMDb Enriched: {len(omdb_data):,}" if omdb_data else "  • OMDb Enriched: 0")
    print(f"  • Data Source: Top-rated movies from TMDB")
    
    return True

if __name__ == "__main__":
    try:
        success = main()
        if success:
            sys.exit(0)
        else:
            sys.exit(1)
    except KeyboardInterrupt:
        print("\n\nAnalysis interrupted by user")
        sys.exit(1)
    except Exception as e:
        print(f"\nUnexpected error: {e}")
        sys.exit(1)
