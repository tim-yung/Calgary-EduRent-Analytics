import asyncio
from dataclasses import dataclass, field, asdict
from selectolax.parser import HTMLParser
from rich import print
from time import perf_counter
from typing import Optional
import json
import httpx
from models import School_db


@dataclass
class School:
    school_id: int = 0
    name: str = ''
    address: str = ''
    phone: str = ''
    fax: str = ''
    email: str = ''
    website: str = ''
    school_hour: str = ''
    grades: str = ''
    ward: str = ''
    area: str = ''
    total_enrolment: int = 0
    programs_list: list = field(default_factory=list)
    desc: str = ''
    kindergarten_enrolment: Optional[int] = 0
    grade_1_enrolment: Optional[int] = 0
    grade_2_enrolment: Optional[int] = 0
    grade_3_enrolment: Optional[int] = 0
    grade_4_enrolment: Optional[int] = 0
    grade_5_enrolment: Optional[int] = 0
    grade_6_enrolment: Optional[int] = 0
    grade_7_enrolment: Optional[int] = 0
    grade_8_enrolment: Optional[int] = 0
    grade_9_enrolment: Optional[int] = 0
    grade_10_enrolment: Optional[int] = 0
    grade_11_enrolment: Optional[int] = 0
    grade_12_enrolment: Optional[int] = 0
    attendance_area: list = field(default_factory=list)
    walk_zone: list = field(default_factory=list)

def extract_table(html, table_selector, heading_selector, data_selector):

    # Select all the rows in the table
    rows = html.css(table_selector)

    # Extract enrolment data
    enrol_data = {}
    for row in rows:
        heading = row.css(heading_selector)[0].text(
            strip=True) if row.css(heading_selector) else None
        data = row.css(data_selector)[0].text(
            strip=True) if row.css(data_selector) else None
        if heading and data:
            enrol_data[heading] = int(data)

    return enrol_data


def parse_details(html, school_id, attendance_area, walk_zone):

    grade_to_attr = {
        'Kindergarten': 'kindergarten_enrolment',
        'Grade 1': 'grade_1_enrolment',
        'Grade 2': 'grade_2_enrolment',
        'Grade 3': 'grade_3_enrolment',
        'Grade 4': 'grade_4_enrolment',
        'Grade 5': 'grade_5_enrolment',
        'Grade 6': 'grade_6_enrolment',
        'Grade 7': 'grade_7_enrolment',
        'Grade 8': 'grade_8_enrolment',
        'Grade 9': 'grade_9_enrolment',
        'Grade 10': 'grade_10_enrolment',
        'Grade 11': 'grade_11_enrolment',
        'Grade 12': 'grade_12_enrolment',
    }
    table_data = extract_table(
        html, '.table-enrol-num tr', '.enrol-heading', '.enrol-data')
    enrolment_fields = {grade_to_attr[grade]: enrolment for grade,
                        enrolment in table_data.items() if grade in grade_to_attr}

    new_school = School(
        school_id=school_id,
        name=html.css(
            'div#page-title')[0].text(strip=True) if html.css('div#page-title') else 'none',
        address=html.css('span#ctl00_PlaceHolderMain_lblAddress')[0].text(
            strip=True) if html.css('span#ctl00_PlaceHolderMain_lblAddress') else 'none',
        phone=html.css('span#ctl00_PlaceHolderMain_lblPhone')[0].text(
            strip=True) if html.css('span#ctl00_PlaceHolderMain_lblPhone') else 'none',
        fax=html.css('span#ctl00_PlaceHolderMain_lblFax')[0].text(
            strip=True) if html.css('span#ctl00_PlaceHolderMain_lblFax') else 'none',
        email=html.css('a#ctl00_PlaceHolderMain_hlEmail')[0].text(
            strip=True) if html.css('a#ctl00_PlaceHolderMain_hlEmail') else 'none',
        website=html.css('a#ctl00_PlaceHolderMain_hlWebSite')[0].text(
            strip=True) if html.css('a#ctl00_PlaceHolderMain_hlWebSite') else 'none',
        school_hour=html.css('span#ctl00_PlaceHolderMain_lblHours')[0].text(
            strip=True) if html.css('span#ctl00_PlaceHolderMain_lblHours') else 'none',
        grades=html.css('span#ctl00_PlaceHolderMain_lblGrades')[0].text(
            strip=True) if html.css('span#ctl00_PlaceHolderMain_lblGrades') else 'none',
        ward=html.css('span#ctl00_PlaceHolderMain_lblWard')[0].text(
            strip=True) if html.css('span#ctl00_PlaceHolderMain_lblWard') else 'none',
        area=html.css('span#ctl00_PlaceHolderMain_lblArea')[0].text(
            strip=True) if html.css('span#ctl00_PlaceHolderMain_lblArea') else 'none',
        total_enrolment=html.css('span#ctl00_PlaceHolderMain_lblTotalEnrolment')[0].text(
            strip=True) if html.css('span#ctl00_PlaceHolderMain_lblTotalEnrolment') else 'none',
        programs_list=[li.text(strip=True) for li in html.css(
            '#programs > div.programs-list > ul > li')] if html.css('#programs > div.programs-list > ul > li') else [],
        desc=html.css('span#ctl00_PlaceHolderMain_lblDescription')[0].text(
            strip=True) if html.css('span#ctl00_PlaceHolderMain_lblDescription') else 'none',
        **enrolment_fields,
        attendance_area = attendance_area,
        walk_zone = walk_zone

    )
    print(f'Fetched {new_school.name}')
    #sleep(1)
    return new_school


# Function to extract single text from input html
def extract_text(html, selector, index):
    try:
        return html.css(selector)[index].text(strip=True)
    except IndexError:
        return 'none'


async def detail_page_loop(client, headers, school_ids):
    url = "https://www.cbe.ab.ca/schools/school-directory/_layouts/15/cbe.service.spm/viewprofile.aspx"

    tasks = []
    for school_id in school_ids:
        tasks.append(fetch_school_data(client, headers, url, school_id))
    school_info = await asyncio.gather(*tasks)
    return school_info

async def fetch_school_data(client, headers, url, school_id):
    querystring = {"id": school_id}
    resp = await client.get(url, headers=headers, params=querystring)
    html = HTMLParser(resp.text)
    attendance_area, walk_zone = await get_polygon(client,headers,school_id)
    return parse_details(html, school_id, attendance_area, walk_zone)

async def get_school_ids(client, headers):
    url = 'https://www.cbe.ab.ca/schools/school-directory/Pages/default.aspx'
    resp = await client.get(url, headers=headers)
    html = HTMLParser(resp.text)
    school_ids = []

    for id in html.css('tr.cbe-sd-schoollist-item'):
        school_ids.append(id.attributes['data-id'])
    print(f'Total number of schools: {len(school_ids)}')
    return school_ids

async def get_polygon(client, headers,school_id):
    querystring = {"id": school_id}
    url = 'https://www.cbe.ab.ca/schools/find-a-school/_layouts/15/SchoolProfileManager/SchoolProfileManager.asmx/GetSchoolOverlays'
    resp = await client.post(url, headers=headers, json=querystring)
    data = json.loads(resp.text)
    
    # Initialize variables
    attendance_area = []
    walk_zone = []

    # Process the data
    for geo_data in data['d']:
        if geo_data['Type'] == 2:
            attendance_area = geo_data['Polygons']
        elif geo_data['Type'] == 5:
            walk_zone = geo_data['Polygons']
    #print(f'attendance = {len(attendance_area)}')
    #print(f'walk zone = {walk_zone}')
    return attendance_area, walk_zone

def fetch_db_school_data(db):

    # Fetch all school data from the database
    school_data_raw = db.read_schools()

    # Initialize an empty list to store the School objects
    schools = []

    # Iterate through each school tuple in the list
    for data in school_data_raw:
        # Fetch attendance_areas and walk_zones data for each school
        attendance_areas_raw = db.read_attendance_areas(data[0]) # data[0] is school_id
        walk_zones_raw = db.read_walk_zones(data[0]) # data[0] is school_id

        # Convert raw data to list of strings (polygons)
        attendance_areas = convert_polygons(attendance_areas_raw)
        walk_zones = convert_polygons(walk_zones_raw)

        # Convert the tuple to a School object and append it to the list
        school = School(school_id=data[0], name=data[1], address=data[2], phone=data[3],
                        fax=data[4], email=data[5], website=data[6], school_hour=data[7],
                        grades=data[8], ward=data[9], area=data[10], total_enrolment=data[11],
                        programs_list=data[12].split(", "), desc=data[13], attendance_area=attendance_areas,
                        walk_zone=walk_zones, kindergarten_enrolment=data[14], grade_1_enrolment=data[15],
                        grade_2_enrolment=data[16], grade_3_enrolment=data[17], grade_4_enrolment=data[18],
                        grade_5_enrolment=data[19], grade_6_enrolment=data[20], grade_7_enrolment=data[21],
                        grade_8_enrolment=data[22], grade_9_enrolment=data[23], grade_10_enrolment=data[24],
                        grade_11_enrolment=data[25], grade_12_enrolment=data[26])
        schools.append(school)
    print(schools)
    # Return the list of School objects
    return schools

def convert_polygons(raw_data):
    # Initialize an empty list to store polygon data
    polygons = []

    # Initialize an empty string to store coordinates of a polygon
    polygon = ""

    # Initialize the polygon_number to 0
    polygon_number = 0

    # Iterate through each tuple in the raw_data
    for data in raw_data:
        # If this is a new polygon, append the previous polygon to polygons list and start a new polygon
        if data[0] != polygon_number:
            polygons.append(polygon.strip())
            polygon = ""
            polygon_number = data[0]
        
        # Append the coordinate to the polygon
        polygon += f"{data[1]} {data[2]}, "

    # Append the last polygon to polygons list
    polygons.append(polygon.strip())
    
    # Return the list of polygons
    return polygons


async def main():
    start = perf_counter()
    async with httpx.AsyncClient(timeout=60.0) as client:
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/117.0.0.0 Safari/537.36"}       
        school_ids = await get_school_ids(client, headers)
        school_list = await detail_page_loop(client, headers, school_ids)
               
        db = School_db()
        for school in school_list:
            db.insert_school(school)
        
    perf = perf_counter() - start
    print(f'Time spent = {perf}')

if __name__ == '__main__':
    asyncio.run(main())
