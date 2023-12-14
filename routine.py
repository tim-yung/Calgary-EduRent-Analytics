import load_listing, load_catchment, load_crime
from time import perf_counter
from datetime import datetime
from rich import print

def main():
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
    