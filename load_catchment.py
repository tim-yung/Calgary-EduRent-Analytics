import polygon_module
import numpy as np
import multiprocessing
from concurrent.futures import ProcessPoolExecutor
import concurrent.futures
from time import perf_counter
import sqlite3
from loguru import logger

# Function to get all school IDs
def get_all_school_ids(cur):
    try:
        cur.execute("""SELECT school_id FROM schools""")
        rows = cur.fetchall()
        return rows
    except sqlite3.Error as e:
        logger.exception(f"An error occurred in get_all_school_ids: {e}")

# Function to read all attendance areas
def read_all_attendance_areas(cur):
    try:
        cur.execute("""SELECT school_id, polygon_number, lat_coordinate, long_coordinate FROM attendance_areas ORDER BY school_id, attendance_area_id""")
        rows = cur.fetchall()

        all_polygons = {}
        for row in rows:
            school_id, polygon_number, lat, long = row
            if school_id not in all_polygons:
                all_polygons[school_id] = {}
            if polygon_number not in all_polygons[school_id]:
                all_polygons[school_id][polygon_number] = []

            all_polygons[school_id][polygon_number].append((lat, long))

        return all_polygons
    except sqlite3.Error as e:
        logger.exception(f"An error occurred in read_all_attendance_areas: {e}")

# Function to read all walk zones
def read_all_walk_zones(cur):
    try:
        cur.execute("""SELECT school_id, polygon_number, lat_coordinate, long_coordinate FROM walk_zones ORDER BY school_id, walk_zone_id""")
        rows = cur.fetchall()

        all_polygons = {}
        for row in rows:
            school_id, polygon_number, lat, long = row
            if school_id not in all_polygons:
                all_polygons[school_id] = {}
            if polygon_number not in all_polygons[school_id]:
                all_polygons[school_id][polygon_number] = []

            all_polygons[school_id][polygon_number].append((lat, long))

        return all_polygons
    except sqlite3.Error as e:
        logger.exception(f"An error occurred in read_all_walk_zones: {e}")

# Function to get all active listing IDs and coordinates
def get_all_listing_ids_coordinates(cur):
    try:
        cur.execute("""
                    SELECT rl.id, rl.latitude, rl.longitude
                    FROM rental_listings AS rl
                    LEFT JOIN schools_within_catchment AS swc ON rl.id = swc.listing_id
                    LEFT JOIN schools_within_walk_zone AS swwz ON rl.id = swwz.listing_id
                    WHERE rl.is_active = True
                    AND swc.listing_id IS NULL
                    AND swwz.listing_id IS NULL;
                    """)
        rows = cur.fetchall()
        logger.debug(f"Number of new rental listings to be processed: {len(rows)}")
        return rows
    except sqlite3.Error as e:
        logger.exception(f"An error occurred in get_all_listing_ids_coordinates: {e}")


def process_listings_chunk(listings_chunk, all_school_ids, all_attendance_areas, all_walk_zones, chunk_index):
    insert_attendance_area_records = []
    insert_walk_zone_records = []
    for i, (listing_id, listing_lat, listing_long) in enumerate(listings_chunk):
        for school_id in all_school_ids:
            #check attendnace area
            attendance_areas_dict = all_attendance_areas.get(school_id[0])
            if attendance_areas_dict and polygon_module.check_user_in_polygons(attendance_areas_dict, listing_lat, listing_long):
                #logger.debug(f'Inserting Listing ID: {int(listing_id)} and school id: {school_id[0]}')
                insert_attendance_area_records.append((int(listing_id), school_id[0]))
                
                #check walk zone if within attendance area
                #logger.debug(f'Start Checking Walk Zone')
                walk_zones_dict = all_walk_zones.get(school_id[0])
                if walk_zones_dict and polygon_module.check_user_in_polygons(walk_zones_dict, listing_lat, listing_long):
                    #logger.debug(f'Inserting Listing ID: {int(listing_id)} and school id: {school_id[0]}')
                    insert_walk_zone_records.append((int(listing_id), school_id[0]))
                    #logger.debug(f'Finish Checking Walk Zone')
                
            
        if i % 100 == 0:  # Adjust this value based on how often you want to logger.debug progress
            logger.debug(f'Chunk {chunk_index}: Processed {i} / {len(listings_chunk)} listings')
            logger.complete()
    return insert_attendance_area_records, insert_walk_zone_records

def insert_schools_within_catchment(cur):
    


    # Fetch data using the functions
    all_school_ids = get_all_school_ids(cur)
    all_attendance_areas = read_all_attendance_areas(cur)
    all_walk_zones = read_all_walk_zones(cur)
    all_listing_ids_coordinates = get_all_listing_ids_coordinates(cur)

    # Split listings into chunks
    num_chunks = multiprocessing.cpu_count()  # Or other number
    listings_chunks = np.array_split(all_listing_ids_coordinates, num_chunks)

    # Process each chunk in parallel
    with ProcessPoolExecutor() as executor:
        futures = [executor.submit(process_listings_chunk, chunk, all_school_ids, all_attendance_areas,all_walk_zones, i) for i, chunk in enumerate(listings_chunks)]
        
        # Gather results
        insert_attendance_area_records = []
        insert_walk_zone_records = []
        for i, future in enumerate(concurrent.futures.as_completed(futures)):
            attendance_area_records, walk_zone_records = future.result()
            insert_attendance_area_records.extend(attendance_area_records)
            insert_walk_zone_records.extend(walk_zone_records)
            logger.debug(f'Completed future {i + 1} / {num_chunks}')

    return insert_attendance_area_records, insert_walk_zone_records



def main():
    start = perf_counter()
    
    logger.info(f'Start checking attendance areas and walk zones of each rental listing')
    
    # Connect to the SQLite database
    conn = sqlite3.connect('database.db')
    cur = conn.cursor()

    try:
        #analysis
        insert_attendance_area_records, insert_walk_zone_records = insert_schools_within_catchment(cur)
        
        #for checking and debugging
        #logger.debug(f'attendance data looks like: {insert_attendance_area_records[:3]}')
        #logger.debug(f'attendance data looks like: {insert_walk_zone_records[:3]}')
        
        #inserting into database
        logger.debug(f'Inserting schools_within_catchment to database.')
        cur.executemany("""INSERT INTO schools_within_catchment (listing_id, school_id) VALUES (?, ?)""", insert_attendance_area_records)
        
        logger.debug(f'Inserting schools_within_walk_zone to database.')
        cur.executemany("""INSERT INTO schools_within_walk_zone (listing_id, school_id) VALUES (?, ?)""", insert_walk_zone_records)

        conn.commit()
        logger.info(f'Successfully loaded data into the database.')
        
    except Exception as e:
        logger.exception(f'Error occurred during data load - {e}. Rolling back changes.')
        conn.rollback()  # Rollback any changes if an error occurs
        raise
    finally:
        # Always ensure the cursor and connection are closed
        if cur:
            cur.close()
        if conn:
            conn.close()
        logger.debug(f'Database connection closed.')
    
    perf = perf_counter() - start
    minutes, seconds = divmod(perf, 60)
    logger.debug(f'Time spent in loading catchment and walk zones = {int(minutes)} minutes {int(seconds)} seconds')



if __name__ == '__main__':    
    main()
    