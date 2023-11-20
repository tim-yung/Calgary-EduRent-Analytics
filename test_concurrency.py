import models
import polygon_module
import numpy as np
import multiprocessing
from concurrent.futures import ProcessPoolExecutor
import concurrent.futures
from time import perf_counter


def process_listings_chunk(listings_chunk, all_school_ids, all_attendance_areas, chunk_index):
    insert_records = []
    for i, (listing_id, listing_lat, listing_long) in enumerate(listings_chunk):
        for school_id in all_school_ids:
            polygon_dict = all_attendance_areas.get(school_id[0])
            if polygon_dict and polygon_module.check_user_in_polygons(polygon_dict, listing_lat, listing_long):
                #print(f'Inserting Listing ID: {int(listing_id)} and school id: {school_id[0]}')
                insert_records.append((int(listing_id), school_id[0]))
        if i % 100 == 0:  # Adjust this value based on how often you want to print progress
            print(f'Chunk {chunk_index}: Processed {i} / {len(listings_chunk)} listings')
    return insert_records

def insert_schools_within_catchment(school_db,listing_db):
    all_school_ids = school_db.get_all_school_ids()
    all_attendance_areas = school_db.read_all_attendance_areas()
    all_listing_ids_coordinates = listing_db.get_all_listing_ids_coordinates()

    # Split listings into chunks
    num_chunks = multiprocessing.cpu_count()  # Or other number
    listings_chunks = np.array_split(all_listing_ids_coordinates, num_chunks)

    # Process each chunk in parallel
    with ProcessPoolExecutor() as executor:
        futures = [executor.submit(process_listings_chunk, chunk, all_school_ids, all_attendance_areas, i) for i, chunk in enumerate(listings_chunks)]
        
        # Gather results
        insert_records = []
        for i, future in enumerate(concurrent.futures.as_completed(futures)):
            insert_records.extend(future.result())
            print(f'Completed future {i + 1} / {num_chunks}')

    return insert_records



# Run the function
def main():

    school_db = models.School_db()

    listing_db = models.Listing_db()

    insert_schools_within_catchment(school_db, listing_db)

    school_db.con.close()
    

if __name__ == '__main__':
    start = perf_counter()
    main()
    perf = perf_counter() - start
    print(f'Time spent in main loop= {perf}')