import load_listing, load_catchment, load_crime
from time import perf_counter
from datetime import datetime
from rich import print
from loguru import logger
import sys


def main(): 
    
    # Configure logger to show only INFO and above levels in the console. Set to "DEBUG" to see the steps in between.
    logger.remove()  # Remove default handler
    logger.add(sys.stderr, level="INFO")
    
    #Save logging to a file    
    logger.add("log/routine.log", level = 'DEBUG', retention="1 week", backtrace=True, diagnose=True, enqueue = True) 
    
    start = perf_counter()
    print(f'{datetime.now()}: Start data update routine')
    
    load_listing.main()
    load_crime.main()
    load_catchment.main()

    perf = perf_counter() - start
    minutes, seconds = divmod(perf, 60)
    print(f'{datetime.now()}: Time spent in data update routine = {int(minutes)} minutes {int(seconds)} seconds')
    
if __name__ == '__main__':    
    main()
    