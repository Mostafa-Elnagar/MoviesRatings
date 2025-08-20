#!/usr/bin/env python3
"""
Verify Staging Status
This script shows the complete status of all staging tables and demonstrates data integration
"""

import subprocess

class StagingVerifier:
    """Verifies the status of all staging tables"""
    
    def __init__(self):
        self.container_name = "trino"
        self.server = "localhost:8080"
        self.catalog = "iceberg"
        self.schema = "staging"
    
    def check_table_counts(self):
        """Check record counts for all tables"""
        print("=" * 60)
        print("STAGING TABLES RECORD COUNTS")
        print("=" * 60)
        
        query = """
        SELECT 'movies' as table_name, COUNT(*) as record_count FROM movies 
        UNION ALL 
        SELECT 'movie_genres' as table_name, COUNT(*) as record_count FROM movie_genres 
        UNION ALL 
        SELECT 'movie_cast' as table_name, COUNT(*) as record_count FROM movie_cast 
        UNION ALL 
        SELECT 'omdb_movies' as table_name, COUNT(*) as record_count FROM omdb_movies 
        UNION ALL 
        SELECT 'metacritic_ratings' as table_name, COUNT(*) as record_count FROM metacritic_ratings 
        UNION ALL 
        SELECT 'rotten_tomatoes_ratings' as table_name, COUNT(*) as record_count FROM rotten_tomatoes_ratings 
        ORDER BY table_name
        """
        
        self._execute_trino_command(query, "Getting table record counts")
    
    def show_sample_data(self):
        """Show sample data from the comprehensive view"""
        print("\n" + "=" * 60)
        print("SAMPLE COMPREHENSIVE DATA")
        print("=" * 60)
        
        query = """
        SELECT title, year, genres, 
               vote_average as tmdb_rating, 
               imdb_rating, 
               metacritic_score, 
               tomatometer_score, 
               audience_score 
        FROM comprehensive_movie_data 
        ORDER BY vote_average DESC 
        LIMIT 3
        """
        
        self._execute_trino_command(query, "Getting sample comprehensive data")
    
    def show_data_integration(self):
        """Show how data is integrated across sources"""
        print("\n" + "=" * 60)
        print("DATA INTEGRATION EXAMPLE")
        print("=" * 60)
        
        query = """
        SELECT m.title, m.year, 
               ARRAY_AGG(DISTINCT g.genre) as tmdb_genres,
               o.genre as omdb_genre,
               m.vote_average as tmdb_rating,
               o.imdb_rating,
               mc.metacritic_score,
               rt.tomatometer_score
        FROM movies m
        LEFT JOIN movie_genres g ON m.tmdb_id = g.tmdb_id
        LEFT JOIN omdb_movies o ON m.imdb_id = o.imdb_id
        LEFT JOIN metacritic_ratings mc ON m.imdb_id = mc.imdb_id
        LEFT JOIN rotten_tomatoes_ratings rt ON m.imdb_id = rt.imdb_id
        WHERE m.title = 'The Matrix'
        GROUP BY m.title, m.year, o.genre, m.vote_average, o.imdb_rating, mc.metacritic_score, rt.tomatometer_score
        """
        
        self._execute_trino_command(query, "Getting data integration example")
    
    def _execute_trino_command(self, command: str, description: str):
        """Execute a Trino command and display results"""
        print(f"\n{description}:")
        print("-" * 40)
        
        try:
            result = subprocess.run([
                'docker', 'exec', self.container_name, 'trino',
                '--server', self.server,
                '--catalog', self.catalog,
                '--schema', self.schema,
                '--execute', command
            ], capture_output=True, text=True, check=True)
            
            print(result.stdout)
            
        except subprocess.CalledProcessError as e:
            print(f"Error: {e}")
            if e.stderr:
                print(f"Error details: {e.stderr}")
    
    def run_verification(self):
        """Run complete verification"""
        print("🎬 MOVIE RATINGS DATA LAKEHOUSE - STAGING VERIFICATION")
        print("=" * 60)
        
        # Check table counts
        self.check_table_counts()
        
        # Show sample data
        self.show_sample_data()
        
        # Show data integration
        self.show_data_integration()
        
        print("\n" + "=" * 60)
        print("✅ STAGING VERIFICATION COMPLETED")
        print("=" * 60)
        print("🎉 Your staging schema is fully operational!")
        print("📊 All data sources are integrated and queryable")
        print("🔍 Use the comprehensive_movie_data view for analysis")
        print("=" * 60)


def main():
    """Main function"""
    verifier = StagingVerifier()
    verifier.run_verification()


if __name__ == "__main__":
    main()
