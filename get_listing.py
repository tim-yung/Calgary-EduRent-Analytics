import httpx
import pandas as pd
from urllib.parse import urlencode
import asyncio
import nest_asyncio
from models import Listing_db

async def fetch_data():
    url = "https://www.rentfaster.ca/api/map.json"
    headers = {
        "cookie": "RFUUID=0b2cd84e-b1d0-48b0-b875-3bffa61e40f5; PHPSESSID=83de974bcdf5b8e0a88ba17f40b97296",
        "content-type": "application/x-www-form-urlencoded",
        "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/118.0.0.0 Safari/537.36"
    }

    types = ["Apartment", "Townhouse", "Mobile", "Vacation Home", "Storage", "Room For Rent", "House", "Loft", "Condo Unit", "Basement", "Parking Spot", "Main Floor", "Duplex", "Acreage"]
    city_id = "ab/calgary"

    dataframes = []
    async with httpx.AsyncClient() as client:
        for type_ in types:
            data = {"type[]": type_, "city_id": city_id}
            payload = urlencode(data)
            try:
                response = await client.post(url, data=payload, headers=headers)
                df = pd.DataFrame.from_records(response.json()['listings'])
                print(f"Number of listing for {type_}: {len(df)}")
                dataframes.append(df)
            except Exception as e:
                print(f"Error fetching data for type {type_}: {e}")

    final_df = pd.concat(dataframes)
    print(f"Total number of listings fetched: {len(final_df)}")
    return final_df

def store_df_to_db(df, db):
    db.deactivate_old_listings()
    for _, row in df.iterrows():
        db.insert_listing(row)

def get_listing():
    nest_asyncio.apply()
    df = asyncio.run(fetch_data())
    listing_db = Listing_db()
    store_df_to_db(df, listing_db)
    listing_db.close_connection()