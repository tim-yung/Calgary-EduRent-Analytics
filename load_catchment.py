import models
import polygon_module
import numpy as np
import multiprocessing
from concurrent.futures import ProcessPoolExecutor
import concurrent.futures
from time import perf_counter
from datetime import datetime


def process_listings_chunk(listings_chunk, all_school_ids, all_attendance_areas, all_walk_zones, chunk_index):
    insert_attendance_area_records = []
    insert_walk_zone_records = []
    for i, (listing_id, listing_lat, listing_long) in enumerate(listings_chunk):
        for school_id in all_school_ids:
            #check attendnace area
            attendance_areas_dict = all_attendance_areas.get(school_id[0])
            if attendance_areas_dict and polygon_module.check_user_in_polygons(attendance_areas_dict, listing_lat, listing_long):
                #print(f'Inserting Listing ID: {int(listing_id)} and school id: {school_id[0]}')
                insert_attendance_area_records.append((int(listing_id), school_id[0]))
                
                #check walk zone if within attendance area
                #print(f'Start Checking Walk Zone')
                walk_zones_dict = all_walk_zones.get(school_id[0])
                if walk_zones_dict and polygon_module.check_user_in_polygons(walk_zones_dict, listing_lat, listing_long):
                    #print(f'Inserting Listing ID: {int(listing_id)} and school id: {school_id[0]}')
                    insert_walk_zone_records.append((int(listing_id), school_id[0]))
                    #print(f'Finish Checking Walk Zone')
                
            
        if i % 100 == 0:  # Adjust this value based on how often you want to print progress
            print(f'{datetime.now()}: Chunk {chunk_index}: Processed {i} / {len(listings_chunk)} listings')
    return insert_attendance_area_records, insert_walk_zone_records

def insert_schools_within_catchment(school_db,listing_db):
    
    all_school_ids = school_db.get_all_school_ids()
    all_attendance_areas = school_db.read_all_attendance_areas()
    all_walk_zones = school_db.read_all_walk_zones()
    all_listing_ids_coordinates = listing_db.get_all_listing_ids_coordinates()

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
            print(f'{datetime.now()}: Completed future {i + 1} / {num_chunks}')

    return insert_attendance_area_records, insert_walk_zone_records



def load_catchment():
    start = perf_counter()
    print(f'{datetime.now()}: Start checking attendance areas and walk zones of each rental listing')
    
    #instantiating database models
    school_db = models.School_db()
    listing_db = models.Listing_db()

    #analysis
    insert_attendance_area_records, insert_walk_zone_records = insert_schools_within_catchment(school_db, listing_db)
    
    #for checking and debugging
    #print(f'attendance data looks like: {insert_attendance_area_records[:3]}')
    #print(f'attendance data looks like: {insert_walk_zone_records[:3]}')
    
    #inserting into database
    print(f'{datetime.now()}: Inserting schools_within_catchment to database.')
    listing_db.cur.executemany("""INSERT INTO schools_within_catchment (listing_id, school_id) VALUES (?, ?)""", insert_attendance_area_records)
    
    print(f'{datetime.now()}: Inserting schools_within_walk_zone to database.')
    listing_db.cur.executemany("""INSERT INTO schools_within_walk_zone (listing_id, school_id) VALUES (?, ?)""", insert_walk_zone_records)
    
    print(f'{datetime.now()}: Committing and Closing db connection')
    listing_db.con.commit()
    school_db.con.close()
    listing_db.con.close()
    perf = perf_counter() - start
    print(f'{datetime.now()}:Time spent in main loop= {perf}')

#this script will be imported as a module
'''if __name__ == '__main__':
    
    main()
    '''