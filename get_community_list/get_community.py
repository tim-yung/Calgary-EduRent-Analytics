import asyncio
from time import perf_counter
from datetime import datetime
from playwright.async_api import async_playwright
from rich import print
import csv

def save_to_csv(data_list, file_path):
    """
    Save a list of data to a CSV file.

    Parameters:
    data_list (list): The list of data to save.
    file_path (str): The path to the CSV file where the data will be saved.
    """
    # Open the file in write mode
    with open(file_path, mode='w', newline='', encoding='utf-8') as file:
        # Create a CSV writer object
        writer = csv.writer(file)

        # Write the data
        for item in data_list:
            # Ensure that each item is a list, even if it's a single value
            if not isinstance(item, list):
                item = [item]
            writer.writerow(item)

async def main():
    start =perf_counter()
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True) #False, slow_mo=500
        print(f'{datetime.now()}: Launched browser')
        context = await browser.new_context()
        #page = await browser.new_page()
        page = await context.new_page()
        
        #allow geolocation permission
        await context.set_geolocation({ 'longitude': 114.0719, 'latitude': 51.0447 })
        await context.grant_permissions(['geolocation'])
        
        #go to page
        await page.goto("https://www.rentfaster.ca/ab/calgary/")
        print(f'{datetime.now()}: Page loaded')
        
        #Click 'Filter'
        await page.locator('div#tab-sidebar-results-wrapper button[ng-click="toggleMapSearchFilters();"]').click()
        print(f'{datetime.now()}: Clicked Filter')
        
        
        #Click 'Neighbourhood'
        await page.locator('div#neighborhood-select>div.selectize-input').click()
        print(f'{datetime.now()}: Clicked Neighbourhood')
        
        #Locate target data
        community_list = await page.locator('#neighborhood-select > div.ui-select-choices.ui-select-dropdown.selectize-dropdown.ng-scope.multi > div > div >div ').all_inner_texts()
        print(f'{datetime.now()}:Target data Located')
        
        #remove blank data
        community_list.remove('')
                
        save_to_csv(community_list, 'get_community_list/community_list.csv') 
        print(f'{datetime.now()}: Finished data extraction. Data length: {len(community_list)}')
        
        await browser.close()
    
    perf = perf_counter() - start
    print(f'{datetime.now()}:Time spent in main loop= {perf}')

asyncio.run(main())