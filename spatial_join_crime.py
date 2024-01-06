import pandas as pd
import geopandas as gpd
import sqlite3
from loguru import logger
from time import perf_counter



def load(conn, cursor, df):
    """
    Loads transformed data into the listing_with_crime table in the database.
    
    :param conn: A SQLite database connection.
    :param cursor: A SQLite cursor object.
    :param df: DataFrame with listing and crime IDs.
    """
    
    try:
        logger.debug(f'Loading transformed data into the database...')
        insert_statement = 'INSERT INTO listing_with_crime (listing_id, crime_id) VALUES (?, ?)'
        records_to_insert = [(row.id, row.row_id) for row in df.itertuples(index=False)]
        cursor.executemany(insert_statement, records_to_insert)
        conn.commit()
        logger.info(f'Successfully loaded mapping of community crime into the database.')
    
    except Exception as e:
        logger.exception(f'Error occurred during data load - {e}. Rolling back changes.')
        conn.rollback()  # Rollback any changes if an error occurs
        raise

       
def main():
    """
    Main execution function:
    1. Reads unprocessed rental listings from the database.
    2. Performs a spatial join with community crime data.
    3. Loads the result back into the database.
    """
    start = perf_counter()
    try:
        conn = sqlite3.connect('database.db')
        # Load rental listings which are not yet mapped with community and crime data
        df_listings = pd.read_sql_query('''
                                        SELECT id,latitude,longitude
                                        FROM rental_listings
                                        WHERE id NOT IN (
                                        SELECT DISTINCT(listing_id) 
                                        FROM listing_with_crime
                                        )
                                        ''', conn)
        total = df_listings.shape[0]
        gdf_listings = gpd.GeoDataFrame(df_listings, geometry=gpd.points_from_xy(df_listings['longitude'], df_listings['latitude']), crs="EPSG:4326")
        logger.debug(f'Found {total} rental listings which are not yet mapped with community and crime data.')
        
        # Load geographic data of community and crime
        community_crime = gpd.read_file("community_boundaries/community_crime.geojson")
        logger.debug('Loaded geographic data of community and crime.')

        # Spatial join
        gdf_listings_crime_merged = gpd.sjoin(gdf_listings, community_crime, how="inner", lsuffix='rl', rsuffix='cc')
        mapped = gdf_listings_crime_merged.shape[0]
        logger.info(f'Mapped {mapped} ({(mapped/total*100):.2f}%) rental listings.')

        # Update database
        cur = conn.cursor()
        load(conn, cur, gdf_listings_crime_merged)
        
    except Exception as e:
        logger.exception(f'An error occurred in the main function: {e}')
        if conn:
            conn.rollback()
            
    finally:
        # Close cursor and connection
        if cur:
            cur.close()
        if conn:
            conn.close()
        logger.debug('Database connection and cursor closed.')
    
    # performance counter
    perf = perf_counter() - start
    minutes, seconds = divmod(perf, 60)
    logger.debug(f'Time spent in loading crime data = {int(minutes)} minutes {int(seconds)} seconds')

if __name__ == '__main__':
    
    main()
    
