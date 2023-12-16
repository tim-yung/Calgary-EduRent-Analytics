import pandas as pd
import sqlite3
from thefuzz import process
from time import perf_counter
from loguru import logger

def extract(conn):
    """
    Extracts new rental listings and crime data from the database.
    
    :param conn: A SQLite connection object.
    :return: A tuple of two DataFrames, one for new listings and one for crime data.
    """
    try:
        logger.debug(f'Obtaining new rental listings for matching...')
        
        #Only getting rows where 'id' is not found in 'listing_id'
        df_new_listings = pd.read_sql_query("""
                                SELECT rl.id, rl.community
                                FROM rental_listings rl
                                LEFT JOIN listing_with_crime lwc ON rl.id = lwc.listing_id
                                WHERE lwc.listing_id IS NULL;
                                """, conn)
        logger.debug(f'New rental listings = {df_new_listings.shape[0]}')
        
        logger.debug(f'Obtaining crime info from database...')
        df_crime = pd.read_sql_query("SELECT * FROM crime", conn)
    
    except Exception as e:
        logger.exception(f'Error occurred during data extraction - {e}')
        raise
    
    return df_new_listings, df_crime

def fuzzy_match(name, names_array):
    """
    Performs a fuzzy match of a name against an array of names.
    
    :param name: The name to match.
    :param names_array: An array of names to match against.
    :return: The best match name from the array, or None if no match is found.
    """
    best_match = process.extractOne(name, names_array, score_cutoff=90)
    return best_match[0] if best_match else None

def transform(df_new_listings, df_crime):
    """
    Transforms new listing data by matching community names with crime data.
    
    :param df_new_listings: DataFrame of new listings.
    :param df_crime: DataFrame of crime data.
    :return: A DataFrame of listings with crime IDs.
    """
    logger.debug(f'Transforming new listings with fuzzy matched community names...')
    df_new_listings['community'] = df_new_listings['community'].str.lower().str.strip()
    new_rows = []

    start_perf = perf_counter()

    for listing in df_new_listings.itertuples():
        matched_community = fuzzy_match(listing.community, df_crime['community'].unique())
        if matched_community:
            crime_id = df_crime[df_crime['community'] == matched_community]['id'].iloc[0]
            new_rows.append({'listing_id': listing.id, 'crime_id': crime_id})

    df_listing_with_crime = pd.DataFrame(new_rows)

    end_perf = perf_counter()
    logger.debug(f'Finished transforming data in {end_perf - start_perf:.4f} seconds. Matched rows: {df_listing_with_crime.shape[0]}')
    return df_listing_with_crime

def load(conn, cursor, df_listing_with_crime):
    """
    Loads transformed data into the listing_with_crime table in the database.
    
    :param conn: A SQLite database connection.
    :param cursor: A SQLite cursor object.
    :param df_listing_with_crime: DataFrame with listing and crime IDs.
    """
    
    try:
        logger.debug(f'Loading transformed data into the database...')
        insert_statement = 'INSERT INTO listing_with_crime (listing_id, crime_id) VALUES (?, ?)'
        records_to_insert = [(row.listing_id, row.crime_id) for row in df_listing_with_crime.itertuples(index=False)]
        cursor.executemany(insert_statement, records_to_insert)
        conn.commit()
        logger.debug(f'Successfully loaded data into the database.')
    
    except Exception as e:
        logger.exception(f'Error occurred during data load - {e}. Rolling back changes.')
        conn.rollback()  # Rollback any changes if an error occurs
        raise

    finally:
        # Always ensure the cursor and connection are closed
        if cursor:
            cursor.close()
        if conn:
            conn.close()
        logger.debug(f'Database connection closed.')
    
def main():
    """
    Main ETL process for matching new rental listings with crime data and loading into the database.
    """
    logger.info(f'Start loading crime data for rental listings.')
    conn = sqlite3.connect('database.db')
    cursor = conn.cursor()
    
    try:
        start = perf_counter()
        
        df_new_listings, df_crime = extract(conn)
        df_listing_with_crime = transform(df_new_listings, df_crime)
        load(conn,cursor, df_listing_with_crime)
        
        perf = perf_counter() - start
        minutes, seconds = divmod(perf, 60)
        logger.info(f'Finished loading crime data for rental listings.')
        logger.debug(f'Time spent in loading crime data = {int(minutes)} minutes {int(seconds)} seconds')
    
    except Exception as e:
        logger.exception(f'An error occurred in the ETL process - {e}')

if __name__ == "__main__":
    main()