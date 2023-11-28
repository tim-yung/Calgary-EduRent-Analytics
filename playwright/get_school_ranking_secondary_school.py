import asyncio
from time import perf_counter
from datetime import datetime
from playwright.async_api import async_playwright, expect
from bs4 import BeautifulSoup
from rich import print
import csv

def save_to_csv(data, filename):
    # Get the keys from the first item, which will be our column headers
    headers = data[0].keys()

    with open(filename, 'w', newline='') as output_file:
        writer = csv.DictWriter(output_file, headers)
        writer.writeheader()
        writer.writerows(data)

async def extract_data(data_list):
    extracted_data = []

    for item in data_list:
        soup = BeautifulSoup(await item.inner_html(), 'html.parser')

        # Define the CSS selectors for each piece of data
        selectors = {
            'school_name': '.school-name.text-xs-left a',
            'rank_detail_url': '.school-name.text-xs-left a',
            'school_rating': 'td:nth-of-type(2)',  # Assuming rating is the second `td` element
            'school_rank': 'td:nth-of-type(3)',  # Assuming rank is the third `td` element
            'city': 'td:nth-of-type(4)'  # Assuming city is the fourth `td` element
        }
        
        # Extract the data
        data_dict = {
            key: (soup.select_one(selector).text.strip() if key != 'rank_detail_url' 
                  else soup.select_one(selector).get('href')) 
            if soup.select_one(selector) else None 
            for key, selector in selectors.items()
        }

        extracted_data.append(data_dict)

    return extracted_data

async def page_loop(page):
    full_result = []
    while True:
        
        #Parsing data
        data_table = await page.locator('div.school-list-view-full table.v-datatable.v-table.theme--light > tbody >tr').all()
        print(f'Length of data table: {len(data_table)}')
        page_result = await extract_data(data_table)
        full_result.extend(page_result)
        
        
        next_page = page.locator('li.next > a',has_text='NEXT')
        check_next_page = await next_page.is_visible()
        #secondary_school = page.locator('div.education-level-secondary > a.v-tabs__item')
        #check_secondary_school = await secondary_school.is_visible()
        print(f'check_next_page: {check_next_page}')
        #print(f'secondary_school: {check_secondary_school}')
        
        if check_next_page:
            await next_page.click()
        else:
            break
    return full_result

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
        await page.goto("https://www.compareschoolrankings.org/")
        print(f'{datetime.now()}: Page loaded')
        
        #wait for pop up
        await expect(page.get_by_placeholder("Please select a province")).to_be_visible()
        print(f'{datetime.now()}: Province list visable')
        
        #Click list to show dropdown Menu
        await page.locator('div.v-select__slot').nth(1).click(delay=200)    
        print(f'{datetime.now()}: Clicked to show province list')
        
        #select 'Alberta'
        await expect(page.locator('.v-list__tile__title',has_text='Alberta').nth(0)).to_be_enabled()
        await page.locator('div.v-list__tile__title',has_text='Alberta').nth(0).click()
        print(f'{datetime.now()}: Selected province')
        
        #wait for page to load        
        await expect(page.locator('.layout.school-map-search-content.row.wrap.justify-space-between')).to_be_visible()
        
        #Click list view
        await page.locator('button.v-btn.v-btn--flat.theme--light',has_text='List view').click()
        print(f'{datetime.now()}: Clicked List View')
        
        await page.get_by_placeholder("Search for a school name, city…").fill("Calgary")
        print(f'{datetime.now()}: Inputted Calgary into the search field')
        
        
        #Select Calgary from dropdown menu
        await page.locator('div[role="listitem"]', has_text='Calgary',has= page.locator('span.v-chip__content',has_text='City')).click()
        print(f'{datetime.now()}: Clicked Calgary from dropdown menu')
        
        #wait for target data to load
        data_table_locator = page.locator('div.school-list-view-full table.v-datatable.v-table.theme--light tbody')
        await expect(data_table_locator).to_be_visible()        
        #extra step to ensure table is fully rendered
        await data_table_locator.hover()
        print(f'{datetime.now()}: Target data loaded')
        
        #get data for primary school
        primary_school_result = await page_loop(page)
        print(f'{datetime.now()}: Fetched element schools ranking')
        
        #click secondary school
        secondary_school = page.locator('div.education-level-secondary > a.v-tabs__item')
        await secondary_school.click()
        print(f'{datetime.now()}: Clicked Secondary School')
        
        await page.locator('div.flex.hidden-sm-and-down.school-map-submit.text-xs-center.md2 > button.v-btn.v-btn--flat.v-btn--round.theme--light').click()
        print(f'{datetime.now()}: Clicked Search button')
        
        #data_table_locator = page.locator('div.school-list-view-full table.v-datatable.v-table.theme--light tbody')
        await expect(data_table_locator).to_be_visible()        
        #extra step to ensure table is fully rendered
        await data_table_locator.hover()
        print(f'{datetime.now()}: Target data loaded')
        
        secondary_school_result = await page_loop(page)
        print(f'{datetime.now()}: Fetched Secondary schools ranking')
        #print(f'Secondary school data: {secondary_school_result}')
        
        
        full_result = primary_school_result + secondary_school_result
        #print(f'Full result: {full_result}')

        #print data for debugging
        '''
        for row in full_result:
            print(row)
        '''
        
        #save to CSV
        save_to_csv(full_result, 'playwright/school_ranking.csv') 
        print(f'{datetime.now()}: Finished data extraction. Data length: {len(full_result)}')
        
        await page.screenshot(path="playwright/example.png", full_page= True)
        await browser.close()
    
    perf = perf_counter() - start
    print(f'{datetime.now()}: Time spent in main loop = {perf}')

asyncio.run(main())