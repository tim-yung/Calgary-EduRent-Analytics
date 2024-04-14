import load_listing, spatial_join_school, spatial_join_crime
from time import perf_counter
from loguru import logger
import sys


def main(): 
    
    # Configure logger to show only INFO and above levels in the console. Set to "DEBUG" to see the steps in between.
    logger.remove()  # Remove default handler
    logger.add(sys.stderr, level="INFO")
    
    #Save logging to a file    
    logger.add("log/routine.log", level = 'DEBUG', retention="1 week", backtrace=True, diagnose=True, enqueue = True) 
    
    start = perf_counter()
    logger.info('Start data update routine')
    
    # Update rental listings in database
    load_listing.main()
    
    # perform spatial join with crime data
    spatial_join_crime.main()
    
    # perform spatial join with walk zones and attendance areas of schools
    spatial_join_school.main()
    

    perf = perf_counter() - start
    minutes, seconds = divmod(perf, 60)
    logger.info(f'Time spent in data update routine = {int(minutes)} minutes {int(seconds)} seconds')
    
if __name__ == '__main__':    
    main()
    