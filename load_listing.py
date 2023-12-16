import httpx
import pandas as pd
from urllib.parse import urlencode
import asyncio
import nest_asyncio
import json
from datetime import datetime
from time import perf_counter
import sqlite3
from loguru import logger

async def fetch_data():
    """
    Asynchronously fetches rental listings data from the specified URL and returns
    a combined DataFrame of all listings.
    """
    
    # Define the base URL and request headers
    url = "https://www.rentfaster.ca/api/map.json"
    headers = {
        "cookie": "RFUUID=0b2cd84e-b1d0-48b0-b875-3bffa61e40f5; PHPSESSID=83de974bcdf5b8e0a88ba17f40b97296",
        "content-type": "application/x-www-form-urlencoded",
        "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/118.0.0.0 Safari/537.36"
    }

    # Define the types of rental properties and the target city
    types = ["Apartment", "Townhouse", "Mobile", "Vacation Home", "Storage", "Room For Rent", "House", "Loft", "Condo Unit", "Basement", "Parking Spot", "Main Floor", "Duplex", "Acreage"]
    city_id = "ab/calgary"

    dataframes = []
    async with httpx.AsyncClient() as client:
        for type_ in types:
            data = {"type[]": type_, "city_id": city_id}
            payload = urlencode(data)
            try:
                # Asynchronously post the request and fetch the response
                response = await client.post(url, data=payload, headers=headers)
                df = pd.DataFrame.from_records(response.json()['listings'])
                logger.debug(f"Number of listing for {type_}: {len(df)}")
                dataframes.append(df)
            except Exception as e:
                logger.exception(f"Error fetching data for type {type_}: {e}")

    # Combine all DataFrames and remove duplicates
    final_df = pd.concat(dataframes)
    final_df.drop_duplicates(inplace=True,subset = 'id')
    logger.info(f"Total number of listings with unique 'id' fetched: {len(final_df)}")
    return final_df

def transform_df(df_listings):
    """
    Transforms the DataFrame of listings by adding a last_update timestamp
    and setting is_active to True. Also converts utilities_included to JSON strings.
    
    :param df_listings: The DataFrame containing rental listings.
    :return: The transformed DataFrame.
    """
    
    df_listings['last_update'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    df_listings['is_active'] = True      

    # Convert 'utilities_included' lists to JSON strings
    df_listings['utilities_included'] = df_listings['utilities_included'].apply(json.dumps)
    return df_listings

def load_to_db(df_listings):
    """
    Loads the transformed DataFrame of listings into a SQLite database,
    updating existing records and inserting new ones.
    
    :param df_listings: The DataFrame containing transformed rental listings.
    """
    
    # Connect to the SQLite database
    conn = sqlite3.connect('database.db')
    cursor = conn.cursor()

    # Create table if not exists
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS rental_listings (
        ref_id INTEGER,
        id INTEGER PRIMARY KEY,
        userId INTEGER,
        phone TEXT,
        phone_2 TEXT,
        email TEXT,
        availability TEXT,
        a TEXT,
        v INTEGER,
        f INTEGER,
        s TEXT,
        title TEXT,
        intro TEXT,
        city TEXT,
        community TEXT,
        latitude REAL,
        longitude REAL,
        marker TEXT,
        link TEXT,
        thumb2 TEXT,
        preferred_contact TEXT,
        type TEXT,
        price INTEGER,
        price2 INTEGER,
        beds INTEGER,
        beds2 INTEGER,
        sq_feet INTEGER,
        sq_feet2 INTEGER,
        baths INTEGER,
        baths2 INTEGER,
        cats REAL,
        dogs REAL,
        utilities_included TEXT,
        last_update TEXT,
        is_active BOOLEAN
    )
    ''')
    
    
    # Begin transaction
    try:
        # Get all existing 'id' from the table
        cursor.execute("SELECT DISTINCT(id) FROM rental_listings")
        existing_ids = [row[0] for row in cursor.fetchall()]
        logger.debug(f'Retrieved existing_ids')

        # Filter the DataFrame
        df_update = df_listings[df_listings['id'].isin(existing_ids)]
        df_insert = df_listings[~df_listings['id'].isin(existing_ids)]
        logger.debug(f'Number of existing rows to be updated: {len(df_update)}')
        logger.debug(f'Number of new rows to be updated: {len(df_insert)}')
        
        
        # Update is_active to False for all current records
        cursor.execute("UPDATE rental_listings SET is_active = ?, last_update = ? WHERE is_active = ?", (False, datetime.now().strftime('%Y-%m-%d %H:%M:%S'), True))
        logger.debug(f'Updated is_active to False for all current records')
        
        # Prepare the list of values to update
        values_to_update = []
        for row in df_update.itertuples(index=False):
            row_values = (row.ref_id,) + row[2:]  # This skips the 'id' column 
            update_tuple = row_values + (row.id,)  # Append the 'id' for the WHERE clause
            values_to_update.append(update_tuple)
        
        
        # Define the update query with placeholders for each column value
        update_query = '''
        UPDATE rental_listings SET
            ref_id = ?,
            userId = ?,
            phone = ?,
            phone_2 = ?,
            email = ?,
            availability = ?,
            a = ?,
            v = ?,
            f = ?,
            s = ?,
            title = ?,
            intro = ?,
            city = ?,
            community = ?,
            latitude = ?,
            longitude = ?,
            marker = ?,
            link = ?,
            thumb2 = ?,
            preferred_contact = ?,
            type = ?,
            price = ?,
            price2 = ?,
            beds = ?,
            beds2 = ?,
            sq_feet = ?,
            sq_feet2 = ?,
            baths = ?,
            baths2 = ?,
            cats = ?,
            dogs = ?,
            utilities_included = ?,
            last_update = ?,
            is_active = ?
        WHERE id = ?
        '''

        # Use executemany() to update the records more efficiently
        cursor.executemany(update_query, values_to_update)
        logger.debug(f'Finished updating existing records')
        
        # Insert new records
        insert_columns = list(df_listings.columns)
        insert_values_placeholder = ','.join('?' * len(insert_columns))
        insert_query = f'INSERT INTO rental_listings ({",".join(insert_columns)}) VALUES ({insert_values_placeholder})'
        #print(f'insert_query looks like:{insert_query}')
        cursor.executemany(insert_query, df_insert.itertuples(index=False))
        logger.debug(f'Finished inserting new records')

        # Commit if no errors
        conn.commit()
        logger.debug(f'COMMIT')
    except sqlite3.Error as e:
        # Rollback on any error
        logger.exception(f"An error occurred: {e}")
        conn.rollback()
        logger.debug(f'ROLLBACK')
    finally:
        # Close the cursor and connection
        
        cursor.close()
        conn.close()

    

def main():
    """
    Executes the ETL process by fetching, transforming, and loading the rentallistings data into the database. It measures the performance time and ensures
    that the asyncio event loop works in a Jupyter notebook environment.
    """
    
    #Start timer
    start = perf_counter()
    logger.info(f'Start updating rental_listings table.')
    
    # Main loop
    # Enable asyncio to run in a Jupyter notebook by applying the necessary patch
    nest_asyncio.apply()
    df_listings = asyncio.run(fetch_data())
    df_listings = transform_df(df_listings)
    load_to_db(df_listings)
    
    #End timer
    perf = perf_counter() - start
    minutes, seconds = divmod(perf, 60)
    logger.info(f'Finished updating rental_listings table.')
    logger.debug(f'Time spent in the main loop = {int(minutes)} minutes {int(seconds)} seconds')
    

if __name__ == '__main__':
    main()