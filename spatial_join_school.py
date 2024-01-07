import pandas as pd
import geopandas as gpd
import sqlite3
from shapely.geometry import Polygon, MultiPolygon
from loguru import logger
from time import perf_counter


def transform_to_geometry(df):
    """
    Transform a DataFrame with individual coordinate points into a GeoDataFrame with polygon geometries.
    
    This function takes a DataFrame that includes school IDs, names, polygon numbers, and pairs
    of latitude and longitude coordinates. It groups the points by school and polygon number to
    create polygons, and then creates a MultiPolygon for schools with multiple polygons.
    The resulting GeoDataFrame has one row per school with a geometry column containing the 
    corresponding MultiPolygon or Polygon.
    
    Parameters:
    - df (pd.DataFrame): A DataFrame with the following columns:
        - 'school_id': An identifier for the school.
        - 'name': The name of the school.
        - 'polygon_number': An identifier for a polygon (in case a school has multiple polygons).
        - 'long_coordinate': Longitude part of the coordinate.
        - 'lat_coordinate': Latitude part of the coordinate.
        
    Returns:
    - gpd.GeoDataFrame: A GeoDataFrame with the following columns:
        - 'school_id': An identifier for the school.
        - 'name': The name of the school.
        - 'geometry': A shapely.geometry.Polygon or shapely.geometry.MultiPolygon object representing the school's geometry.
    """
    
    # First, create a new DataFrame for polygons
    df_polygons = df.groupby(['school_id', 'name', 'polygon_number']).apply(
        lambda group: Polygon(zip(group['long_coordinate'], group['lat_coordinate']))
    ).reset_index().rename(columns={0: 'geometry'})
    
    # Convert our DataFrame with Polygon objects into a GeoDataFrame
    gdf_polygons = gpd.GeoDataFrame(df_polygons, geometry='geometry',crs="EPSG:4326")
    
    # Now use dissolve to merge the polygons into MultiPolygons where necessary
    gdf = gdf_polygons.dissolve(by=['school_id', 'name'], as_index=False)
    
    # Ensure the geometry column contains only MultiPolygon instances
    #gdf['geometry'] = gdf.apply(lambda row: MultiPolygon([row.geometry]) if type(row.geometry) is Polygon else row.geometry, axis=1)

    gdf.drop(columns = ['polygon_number'],inplace=True)
    return gdf


def load(conn, cursor, df, table_name):
    """
    Loads transformed data into the specified table in the database.
    
    :param conn: A SQLite database connection.
    :param cursor: A SQLite cursor object.
    :param df: DataFrame with listing and school IDs.
    :param table_name: Name of the database table to insert data into.
    """
    
    try:
        logger.info(f'Loading transformed data into {table_name} of the database...')
        insert_statement = f'INSERT INTO {table_name} (listing_id, school_id) VALUES (?, ?)'
        records_to_insert = [(row.id, row.school_id) for row in df.itertuples(index=False)]
        cursor.executemany(insert_statement, records_to_insert)
        conn.commit()
        logger.info(f'Successfully loaded data into {table_name} of the database.')
    
    except Exception as e:
        logger.exception(f'Error occurred during data load in {table_name} - {e}. Rolling back changes.')
        conn.rollback()  # Rollback any changes if an error occurs
        raise

def process_zone(conn, zone_type):
    """
    Process either attendance area or walk zone.
    
    :param conn: A SQLite database connection.
    :param zone_type: Type of zone to process ('attendance_area' or 'walk_zones').
    """
    table_name = f'schools_within_{zone_type}'

    # Load rental listings that do not have mapping of schools
    df_listings = pd.read_sql_query(f'''
                                    SELECT id,latitude,longitude
                                    FROM rental_listings
                                    WHERE id NOT IN (
                                    SELECT DISTINCT(listing_id) 
                                    FROM {table_name})
                                    ''', conn)
    
    total = df_listings.shape[0]
    gdf_listings = gpd.GeoDataFrame(df_listings, geometry=gpd.points_from_xy(df_listings['longitude'], df_listings['latitude']), crs="EPSG:4326")
    logger.debug(f'Found {total} rental listings which are not yet mapped with {zone_type}')
    
    # Load zones from database.        
    df_z = pd.read_sql_query(f'''SELECT s.school_id, s.name, z.polygon_number, z.lat_coordinate, z.long_coordinate
                                FROM {zone_type}s z
                                INNER JOIN schools s on s.school_id = z.school_id
                                ''', conn)
    logger.debug(f'Loaded {zone_type}.')

    # Transform to geographic data
    gdf_z_t = gpd.GeoDataFrame(transform_to_geometry(df_z),
                                geometry='geometry', 
                                crs="EPSG:4326")
    
    # Spatial join
    gdf_z_listings = gpd.sjoin(gdf_listings,gdf_z_t, how="inner",lsuffix='l',rsuffix='r')
    mapped = gdf_z_listings.shape[0]
    logger.info(f'Created {mapped} mappings between {zone_type} and rental listings.')


    cur = conn.cursor()
    load(conn, cur, gdf_z_listings, table_name)

def main():
    """
    Main execution function that processes both attendance areas and walk zones.
    """
    start = perf_counter()
    conn = None
    cur = None
    
    try:
        conn = sqlite3.connect('database.db')
        for zone_type in ['attendance_area', 'walk_zone']:
            process_zone(conn, zone_type)
            
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
    logger.debug(f'Time spent in mapping schools within attendance area and walk zones = {int(minutes)} minutes {int(seconds)} seconds')



if __name__ == '__main__':
    
    main()
    
