import httpx
import pandas as pd
import numpy as np
from urllib.parse import urlencode
import asyncio
from datetime import datetime
from time import perf_counter
import sqlite3
import csv
from loguru import logger
import pandera as pa
from pandera.typing import DataFrame, Series
import re

################
# Load community list
################
# for reading community list and debugging
def load_from_csv(file_path, column=0):
    """
    Load data from a CSV file into a list.

    Parameters:
    file_path (str): The path to the CSV file to read from.
    column (int): The column index to extract data from.

    Returns:
    list: A list containing the data from the specified CSV column.
    """
    data_list = []

    # Open the file in read mode
    with open(file_path, mode='r', newline='', encoding='utf-8') as file:
        # Create a CSV reader object
        reader = csv.reader(file)

        # Read the data
        for row in reader:
            if row:  # Ensure that the row is not empty
                data_list.append(row[column])

    return data_list

COMM_LIST = load_from_csv('get_community_list/community_list.csv')

################
# Declare DataFrameModel for data validation
################
class ExtractSchema(pa.DataFrameModel):
    """
    Schema for rental listings fetched from API
    """
    id: Series[int] == pa.Field(nullable=False, unique=True) # as primary key, must be unique integer and not null
    city: Series[str] = pa.Field(nullable=False) # to ensure the data retrieved is for the correct city, so not null
    community: Series[str] = pa.Field(nullable=False, isin=COMM_LIST) # essential for mapping crime rate, so not null
    latitude: Series[float] = pa.Field(nullable=False, ge=-90, le=90) # essential for matching schools, so not null, should be between -90 and 90
    longitude: Series[float] = pa.Field(nullable=False, ge=-180, le=180) # essential for matching schools, so not null, should be between -180 and 180
    link: Series[str] = pa.Field(nullable=False, str_startswith= '/ab/calgary/rentals/') # essential for user to check out the listing on website, so not null, must start with '/ab/calgary/rentals/'
    type: Series[str] = pa.Field(nullable=False) # essential for filtering, so not null
    price: Series[int] = pa.Field(nullable=False, coerce= True) # main attribute for analysis, so reject null values
    beds: Series[str] = pa.Field(nullable=True,coerce=True) # Optional feature
    sq_feet: Series[str] = pa.Field(nullable=True,coerce=True) # Optional feature 
    baths: Series[float] = pa.Field(nullable=True,ge=0,le=10)# Optional feature, set range between 0 and 10
    cats: Series[float]= pa.Field(nullable=True,ge=0,le=2)# Optional feature, set range between 0 and 2 inferred from data
    dogs: Series[float]= pa.Field(nullable=True,ge=0,le=2)# Optional feature, set range between 0 and 2 inferred from data
    
    class Config:
        drop_invalid_rows = True
        strict = True #make sure all specified columns are in the validated dataframe

class TransformSchema(pa.DataFrameModel):
    """
    Schema for rental listings after data cleaning
    """
    id: Series[int] == pa.Field(nullable=False, unique=True) # as primary key, must be unique integer and not null
    city: Series[str] = pa.Field(nullable=False) # to ensure the data retrieved is for the correct city, so not null
    community: Series[str] = pa.Field(nullable=False) # essential for mapping crime rate, so not null
    latitude: Series[float] = pa.Field(nullable=False) # essential for matching schools, so not null
    longitude: Series[float] = pa.Field(nullable=False) # essential for matching schools, so not null
    link: Series[str] = pa.Field(nullable=False) # essential for user to check out the listing on website, so not null
    type: Series[str] = pa.Field(nullable=False) # essential for filtering, so not null
    price: Series[int] = pa.Field(nullable=False, coerce = True) # main attribute for analysis, so reject null values
    beds: Series[int] = pa.Field(nullable=True) # Optional feature
    has_den: Series[bool] = pa.Field(nullable=False) # new columns from data cleaning
    sq_feet: Series[int] = pa.Field(nullable=True) # accepts None or pd.NA 
    baths: Series[float] = pa.Field(default = 0,ge =0,le=10, nullable=True)# Optional feature 
    cats: Series[bool]= pa.Field(nullable=True)# Optional feature 
    dogs: Series[bool]= pa.Field(nullable=True)# Optional feature
    activation_date: Series[datetime] = pa.Field(nullable=False) # essential
    last_update: Series[datetime] = pa.Field(nullable=False) # essential
    is_active: Series[bool]= pa.Field(nullable=False)# essential
    
    
    class Config:
        drop_invalid_rows = False
        strict = True #make sure all specified columns are in the validated dataframe

################
# Data extraction
################

async def fetch_data_for_community(client, community, url, headers):
    """
    Asynchronously fetch rental data for a specific community using the provided url and headers.
    
    Args:
    client (httpx.AsyncClient): The HTTP client for making requests.
    community (str): The community name for which the data is being fetched.
    url (str): The endpoint URL for fetching data.
    headers (dict): The HTTP headers to be sent with the request.
    
    Returns:
    pandas.DataFrame: A DataFrame containing the rental data for the community.
    """
    
    # Prepare the data payload with the community parameter for the POST request
    data = {"neighborhood[]": community}
    payload = urlencode(data)
    
    try:
        # Send the POST request
        response = await client.post(url, data=payload, headers=headers)
        # Convert the JSON response into a DataFrame
        df = pd.DataFrame.from_records(response.json()['listings'])
        
        logger.debug(f"Number of listings for {community}: {len(df)}")
        
        # If there are listings, select specific columns
        if not df.empty:
            return df[['id', 'city', 'community', 'latitude', 'longitude', 'link', 'type', 'price', 'beds', 'sq_feet', 'baths', 'cats', 'dogs']]
        else:
            # If the DataFrame is empty, return an empty DataFrame with no columns
            # This is useful for concatenating results later without extra checks
            return pd.DataFrame()
    except Exception as e:
        # Log the exception if the request fails
        logger.exception(f"Error fetching data for {community}: {e}")
        return pd.DataFrame()

@pa.check_types(lazy=True)
async def fetch_data() -> DataFrame[ExtractSchema]:
    """
    Fetch rental data for a list of communities concurrently and compile it into a single DataFrame.
    
    Returns:
    pandas.DataFrame: A DataFrame containing all the unique rental listings fetched.
    """
    # The URL endpoint for the API request
    url = "https://www.rentfaster.ca/api/map.json"
    # Headers to mimic a user-agent and include other necessary information for the request
    headers = {
        "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "accept-language": "en-US,en;q=0.9,zh-TW;q=0.8,zh;q=0.7",
        "content-type": "application/x-www-form-urlencoded",
        "origin": "https://www.rentfaster.ca",
        "referer": "https://www.rentfaster.ca/"
    }
    
    tasks = []
    # Create an asynchronous HTTP client session
    async with httpx.AsyncClient() as client:
        # Create a list of coroutines for each community in the global COMM_LIST
        tasks = [fetch_data_for_community(client, community, url, headers) for community in COMM_LIST]
        
        # Asynchronously gather all data from the tasks
        dataframes = await asyncio.gather(*tasks)
    
    # Combine all non-empty DataFrames into one and remove duplicate listings based on 'id'
    final_df = pd.concat([df for df in dataframes if not df.empty], ignore_index=True)
    final_df.drop_duplicates(inplace=True, subset='id')
    
    # Log the total number of unique listings fetched
    logger.info(f"Total number of listings with unique 'id' fetched: {len(final_df)}")
    
    # Save the final DataFrame to a CSV file for debugging
    final_df.to_csv('listing_df_raw.csv')
    
    # Return the final compiled DataFrame
    return final_df
################
# Data Cleaning and Transformation
################

# Function to clean 'beds' column
def clean_beds(df):
    """
    Cleans the 'beds' column in a DataFrame by converting it to integers and adding a 'has_den' boolean column.
    
    This function updates the 'beds' column by converting all entries to integers, with 'studio' and 'None' being treated as 0.
    A new boolean column 'has_den' is added to indicate whether the original entry included a '+den'.
    
    Parameters:
    df (pd.DataFrame): The DataFrame with the 'beds' column to clean.
    
    Returns:
    pd.DataFrame: The DataFrame with the cleaned 'beds' column and the new 'has_den' column.
    """
    # Create 'has_den' column based on the presence of '+den' in the 'beds' column
    df['has_den'] = df['beds'].astype(str).str.contains('+den', regex=False)
    
    # Convert all 'beds' values to strings, then replace '+den' with '' and 'studio' with '0'
    df['beds'] = df['beds'].astype(str).replace({r'\+den': '',
                                                 'studio': '0',
                                                 'None': '0',
                                                 '':'0',
                                                 'Not Listed':'0'}, regex=True)
    
    # Convert the 'beds' column to integer
    df['beds'] = pd.to_numeric(df['beds'], errors='coerce', downcast='integer')
    df['beds'] = df['beds'].astype('Int64')  # need a nullable integer dtype
    
    return df

# Function to clean 'sq_feet column'
def clean_sq_feet(value)-> int or None:
    """
    Cleans the square feet data by handling integers, floats, NoneType, and strings differently.
    NaN is set for NoneType and empty strings. Floats are rounded to the nearest integer.
    Strings are processed using extract_square_feet function to extract numeric value.
    
    Parameters:
    value: The data value to be cleaned.
    
    Returns:
    int or pd.NA: The cleaned square feet as an integer or pd.NA.
    """
    # If the value is None (NoneType), return it as it is
    if value is None or np.nan:
        return pd.NA
    
    # If the value is an integer, return it as it is
    elif isinstance(value, int):
        return value
    
    # If the value is a float, round it to the nearest integer
    elif isinstance(value, float):
        return int(round(value))
    
    # If the value is a string, use the extract_square_feet function
    elif isinstance(value, str):
        # If the string is empty, return pd.NA
        if value.strip() == '':
            return pd.NA
        else:
            return extract_square_feet(value)
    
    # If the value is of any other type, return pd.NA
    else:
        return pd.NA

# Function to clean and extract square footage from strings
def extract_square_feet(value):
    # Regular expression to find the first number in the string with VERBOSE mode for commenting
    pattern = re.compile(r"""
        \d+              # Match one or more digits
        \s*              # Match any whitespace characters (zero or more)
        (?:              # Non-capturing group for the following:
            sq           # Match 'sq'
            (?:uare)?    # Optionally match 'uare' for 'square'
            \.?          # Optionally match a literal '.' for abbreviation
            \s*ft        # Match ' ft' with optional space before 'ft'
            \.?          # Optionally match a literal '.' for abbreviation
        |                # OR
            sf           # Match 'sf' for square feet
        |                # OR
            ft²          # Match 'ft²' for square feet in square notation
        |                # OR
            sqft         # Match 'sqft' for square feet
        )?               # Make the entire group optional
        """, re.VERBOSE)

    # Remove commas for consistency
    value = value.lower().replace(',', '')

    # Search for the pattern in the cleaned string
    match = pattern.search(value)
    
    if match:
        # Extract the number and remove any non-numeric characters
        square_feet = re.sub(r'[^\d]', '', match.group())
        try:
            # Convert to integer
            return int(square_feet)
        except ValueError:
            # If conversion fails, return pd.NA
            return pd.NA
    else:
        # If no number found or if it's a complex description, return pd.NA
        return pd.NA

#Function to clean 'cats' and 'dogs'
def clean_pet(value):
    # Check if the value is NaN or 0.0 and return False, else return True
    return not (pd.isna(value) or value == 0.0)

# Putting all together
@pa.check_types(lazy=True)
def transform_df(df_listings: DataFrame[ExtractSchema])-> DataFrame[TransformSchema]:
    
    transform_df = clean_beds(df_listings)
    logger.debug(f'Cleaned "beds" column and added "has_den" column, dtype ={transform_df["beds"].dtype} {transform_df["has_den"].dtype}')
    
    transform_df['baths'] = pd.to_numeric(transform_df['baths'].replace('None', pd.NA),errors = 'coerce')
    logger.debug(f'Cleaned "baths" column, dtype ={transform_df["baths"].dtype}')
    
    transform_df['sq_feet'] = transform_df['sq_feet'].apply(clean_sq_feet).astype("Int64")
    logger.debug(f'Cleaned "sq_feet" column, dtype ={transform_df["sq_feet"].dtype}')
    
    transform_df['cats'] = transform_df['cats'].apply(clean_pet)
    logger.debug(f'Cleaned "cats" column, dtype ={transform_df["cats"].dtype}')
    
    transform_df['dogs'] = transform_df['dogs'].apply(clean_pet)
    logger.debug(f'Cleaned "dogs" column, dtype ={transform_df["dogs"].dtype}')
    
    transform_df['last_update'] = datetime.now() #.strftime('%Y-%m-%d %H:%M:%S')
    logger.debug(f'Added "last_update" column, dtype ={transform_df["last_update"].dtype}')
    
    transform_df['is_active'] = True    
    logger.debug(f'Added "is_active" column, dtype ={transform_df["is_active"].dtype}')    
    
    transform_df['price'] = transform_df['price'].apply(round).astype(int) #do not cast as "Int64" or it will become a blob
    logger.debug(f'Cleaned "price" column, dtype ={transform_df["price"].dtype}')    
    
    transform_df['activation_date'] = datetime.now() #do not insert this when updating existing ids.
    logger.debug(f'Added "activation_date" column, dtype ={transform_df["last_update"].dtype}')
    
    return transform_df


def load_to_db(df_listings):
    """
    Loads the transformed DataFrame of listings into a SQLite database,
    updating existing records and inserting new ones.
    
    :param df_listings: The DataFrame containing transformed rental listings.
    """
       
    # Connect to the SQLite database
    conn = sqlite3.connect('database.db')
    cursor = conn.cursor()

    # Drop table if exists for testing
    #cursor.execute('''DROP TABLE IF EXISTS rental_listings''')
    #logger.warning('Dropped Table for testing.')
    
    # Create table if not exists
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS rental_listings (
        id INTEGER PRIMARY KEY,
        city TEXT NOT NULL,
        community TEXT NOT NULL,
        latitude REAL NOT NULL,
        longitude REAL NOT NULL,
        link TEXT NOT NULL,
        type TEXT NOT NULL,
        price INTEGER NOT NULL,
        beds INTEGER,
        has_den INTEGER,
        sq_feet INTEGER NULL,
        baths REAL,
        cats INTEGER,
        dogs INTEGER,
        activation_date TEXT NOT NULL,
        last_update TEXT NOT NULL,
        is_active INTEGER NOT NULL
    )
    ''')
    
    
    # Begin transaction
    try:
        # Get all existing 'id' from the table regardless of the is_active flag
        cursor.execute("SELECT DISTINCT(id) FROM rental_listings")
        existing_ids = {row[0] for row in cursor.fetchall()} #a set
        logger.debug(f'Retrieved {len(existing_ids)} existing_ids')

        # Filter the DataFrame
        df_update = df_listings[df_listings['id'].isin(existing_ids)]
        df_insert = df_listings[~df_listings['id'].isin(existing_ids)].copy()
        logger.debug(f'Number of existing rows to be updated: {len(df_update)}')
        logger.debug(f'Number of new rows to be updated: {len(df_insert)}')
        
        
        # Update is_active to False for all existing active records that are not in the incoming data
        inactive_ids = existing_ids - set(df_listings['id'].unique())
        cursor.executemany("UPDATE rental_listings SET is_active = ?, last_update = ? WHERE id = ? ", 
                           [(False, datetime.now().strftime('%Y-%m-%d %H:%M:%S'), id) for id in inactive_ids])
        logger.info(f'Deactivated {len(inactive_ids)} listings not present in incoming data')
        
        # Update existing records
        values_to_update = [(row.city, #1
                             row.community, #2
                             row.latitude, #3
                             row.longitude, #4
                             row.link, #5
                             row.type,  #6
                             row.price, #7
                             None if pd.isna(row.beds) else int(row.beds), #8
                             row.has_den, #9
                             None if pd.isna(row.sq_feet) else int(row.sq_feet), #10
                             None if pd.isna(row.baths) else float(row.baths), #11
                             row.cats,#12
                             row.dogs, #13
                             row.last_update.strftime('%Y-%m-%d %H:%M:%S'), #14 , be sure to skip 'activation_date' here
                             row.is_active, #15
                             row.id) #16
                            for row in df_update.itertuples(index=False)]
        
        update_query = '''
        UPDATE rental_listings SET
            city = ?,
            community = ?,
            latitude = ?,
            longitude = ?,
            link = ?,
            type = ?,
            price = ?,
            beds = ?,
            has_den = ?,
            sq_feet = ?,
            baths = ?,
            cats = ?,
            dogs = ?,
            last_update = ?,
            is_active = ?
        WHERE id = ?
        '''
        cursor.executemany(update_query, values_to_update)
        logger.info(f'Finished updating {len(values_to_update)} existing records')
                
        values_to_insert = [(
            row.id, #1
            row.city, #2
            row.community, #3
            row.latitude, #4
            row.longitude, #5
            row.link, #6
            row.type, #7
            row.price, #8
            None if pd.isna(row.beds) else int(row.beds), #9 handle pd.NA values as None
            row.has_den, #10
            None if pd.isna(row.sq_feet) else int(row.sq_feet), #11 handle pd.NA values as None, use int() to avoid the data being a BLOB
            None if pd.isna(row.baths) else float(row.baths), #12 handle pd.NA values as None
            row.cats, #13
            row.dogs,#14
            row.activation_date.strftime('%Y-%m-%d %H:%M:%S'), #15 convert datetime to string
            row.last_update.strftime('%Y-%m-%d %H:%M:%S'), #16 convert datetime to string
            row.is_active #17
            ) for row in df_insert.itertuples(index= False)]
        
        insert_query = '''
        INSERT INTO rental_listings (
            id,
            city,
            community,
            latitude,
            longitude,
            link,
            type,
            price,
            beds,
            has_den,
            sq_feet,
            baths,
            cats,
            dogs,
            activation_date,
            last_update,
            is_active
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        '''
        
        cursor.executemany(insert_query, values_to_insert)
        logger.info(f'Finished inserting {len(values_to_insert)} new records')

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
    #nest_asyncio.apply()
    
    try:
        df_listings = asyncio.run(fetch_data())
        logger.info(f'Total number of validated listings: {df_listings.shape[0]}')
        df_listings = transform_df(df_listings)
        load_to_db(df_listings)
    except pa.errors.SchemaErrors as err:
        #logger.exception("Schema errors and failure cases:")
        #logger.exception(err.failure_cases)
        logger.exception("\nSaving DataFrame object that failed validation to csv")
        err.data.to_csv(f'log/listing_df_error_{datetime.now().strftime("%Y_%m_%d")}.csv')  
        err.failure_cases.to_csv(f'log/listing_df_failure_cases_{datetime.now().strftime("%Y_%m_%d")}.csv')
    
    #End timer
    perf = perf_counter() - start
    minutes, seconds = divmod(perf, 60)
    #logger.info(f'Finished updating rental_listings table.')
    logger.debug(f'Time spent in the main loop = {int(minutes)} minutes {int(seconds)} seconds')
    

if __name__ == '__main__':
    main()