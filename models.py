import sqlite3
import datetime
import numpy as np
import polygon_module

class School_db:
    def __init__(self):
        self.con = sqlite3.connect('database.db')
        self.cur = self.con.cursor()
        self.create_tables()

    def create_tables(self):
        #self.cur.execute("""DROP TABLE IF EXISTS schools""")
        #self.cur.execute("""DROP TABLE IF EXISTS attendance_areas""")
        #self.cur.execute("""DROP TABLE IF EXISTS walk_zones""")

        self.cur.execute("""
        CREATE TABLE IF NOT EXISTS schools(
            school_id INTEGER PRIMARY KEY,
            name TEXT,
            address TEXT,
            phone TEXT,
            fax TEXT,
            email TEXT,
            website TEXT,
            school_hour TEXT,
            grades TEXT,
            ward TEXT,
            area TEXT,
            total_enrolment INTEGER,
            programs_list TEXT,
            desc TEXT,
            kindergarten_enrolment INTEGER,
            grade_1_enrolment INTEGER,
            grade_2_enrolment INTEGER,
            grade_3_enrolment INTEGER,
            grade_4_enrolment INTEGER,
            grade_5_enrolment INTEGER,
            grade_6_enrolment INTEGER,
            grade_7_enrolment INTEGER,
            grade_8_enrolment INTEGER,
            grade_9_enrolment INTEGER,
            grade_10_enrolment INTEGER,
            grade_11_enrolment INTEGER,
            grade_12_enrolment INTEGER
        )
        """)

        self.cur.execute("""
        CREATE TABLE IF NOT EXISTS attendance_areas(
            attendance_area_id INTEGER PRIMARY KEY AUTOINCREMENT,
            school_id INTEGER,
            polygon_number INTEGER,
            lat_coordinate REAL,
            long_coordinate REAL,
            FOREIGN KEY(school_id) REFERENCES schools(school_id)
        )
        """)

        self.cur.execute("""
        CREATE TABLE IF NOT EXISTS walk_zones(
            walk_zone_id INTEGER PRIMARY KEY AUTOINCREMENT,
            school_id INTEGER,
            polygon_number INTEGER,
            lat_coordinate REAL,
            long_coordinate REAL,
            FOREIGN KEY(school_id) REFERENCES schools(school_id)
        )
        """)

    def insert_school(self, school):
        print(f'Inserting {school.name}')
        school_data = (school.school_id, school.name, school.address, school.phone, school.fax,
                    school.email, school.website, school.school_hour, school.grades, school.ward,
                    school.area, school.total_enrolment, ', '.join(school.programs_list), school.desc,
                    school.kindergarten_enrolment, school.grade_1_enrolment, school.grade_2_enrolment,
                    school.grade_3_enrolment, school.grade_4_enrolment, school.grade_5_enrolment,
                    school.grade_6_enrolment, school.grade_7_enrolment, school.grade_8_enrolment,
                    school.grade_9_enrolment, school.grade_10_enrolment, school.grade_11_enrolment,
                    school.grade_12_enrolment)

        self.cur.execute("""INSERT INTO schools VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""", school_data)

        attendance_areas = []
        for i, polygon in enumerate(school.attendance_area):
            for coordinate in polygon.split(','):
                lat, long = coordinate.split()[:2]
                attendance_areas.append((school.school_id, i, lat, long))

        self.insert_attendance_areas(school.school_id, attendance_areas)

        walk_zones = []
        for i, polygon in enumerate(school.walk_zone):
            for coordinate in polygon.split(','):
                lat, long = coordinate.split()[:2]
                walk_zones.append((school.school_id, i, lat, long))

        self.insert_walk_zones(school.school_id, walk_zones)

        self.con.commit()

    def insert_attendance_areas(self, school_id, areas):
        self.cur.executemany("""INSERT INTO attendance_areas(school_id, polygon_number, lat_coordinate, long_coordinate) VALUES (?, ?, ?, ?)""",
                            areas)

    def insert_walk_zones(self, school_id, zones):  
        self.cur.executemany("""INSERT INTO walk_zones(school_id, polygon_number, lat_coordinate, long_coordinate) VALUES (?, ?, ?, ?)""",
                            zones)

    def read_schools(self):
        self.cur.execute("""SELECT * FROM schools""")
        rows = self.cur.fetchall()
        return rows

    def read_attendance_areas(self, school_id):
        self.cur.execute("""SELECT polygon_number, lat_coordinate, long_coordinate FROM attendance_areas WHERE school_id = ? ORDER BY attendance_area_id""", (school_id,))
        rows = self.cur.fetchall()
        
        # Organize rows by polygon_number
        polygons = {}
        for row in rows:
            if row[0] not in polygons:
                polygons[row[0]] = []
            polygons[row[0]].append((row[1], row[2]))
        return polygons

    def read_all_attendance_areas(self):
        self.cur.execute("""SELECT school_id, polygon_number, lat_coordinate, long_coordinate FROM attendance_areas ORDER BY school_id, attendance_area_id""")
        rows = self.cur.fetchall()

        # Organize rows by school_id and polygon_number
        all_polygons = {}
        for row in rows:
            school_id, polygon_number, lat, long = row
            if school_id not in all_polygons:
                all_polygons[school_id] = {}
            if polygon_number not in all_polygons[school_id]:
                all_polygons[school_id][polygon_number] = []

            all_polygons[school_id][polygon_number].append((lat, long))

        return all_polygons

    def read_walk_zones(self, school_id):
        self.cur.execute("""SELECT polygon_number, lat_coordinate, long_coordinate FROM walk_zones WHERE school_id = ? ORDER BY walk_zone_id""", (school_id,))
        rows = self.cur.fetchall()
        return rows
    
    def get_all_school_ids(self):
        self.cur.execute("""SELECT school_id FROM schools""")
        rows = self.cur.fetchall()
        return rows


class Listing_db:
    def __init__(self):
        self.con = sqlite3.connect('database.db')
        self.cur = self.con.cursor()
        self.create_tables()

    def create_tables(self):
        #self.cur.execute("""DROP TABLE IF EXISTS rental_listings""")
        #self.cur.execute("""DROP TABLE IF EXISTS utilities_included""")
        self.cur.execute("""DROP TABLE IF EXISTS schools_within_catchment""")
        
        self.cur.execute("""
        CREATE TABLE IF NOT EXISTS rental_listings(
            ref_id INTEGER,
            id INTEGER PRIMARY KEY,
            userId INTEGER,
            phone TEXT,
            phone_2 TEXT,
            email TEXT,
            availability TEXT,
            a TEXT,
            v TEXT,
            f TEXT,
            s TEXT,
            title TEXT,
            intro TEXT,
            city TEXT,
            community TEXT,
            latitude REAL,
            longitude REAL,
            marker TEXT,
            link TEXT,
            thumb2 TEXT,
            preferred_contact TEXT,
            type TEXT,
            price REAL,
            price2 REAL,
            beds INTEGER,
            beds2 INTEGER,
            sq_feet INTEGER,
            sq_feet2 INTEGER,
            baths INTEGER,
            cats TEXT,
            dogs TEXT,
            baths2 INTEGER,
            last_update DATE,
            is_active BOOLEAN
        )
        """)
        
        self.cur.execute("""
        CREATE TABLE IF NOT EXISTS price_history(
            id INTEGER PRIMARY KEY,
            listing_id INTEGER,
            price REAL,
            date TIMESTAMP,
            FOREIGN KEY (listing_id) REFERENCES rental_listings (id)
        )
        """)

        self.cur.execute("""
        CREATE TABLE IF NOT EXISTS utilities_included(
            id INTEGER PRIMARY KEY,
            listing_id INTEGER,
            utility TEXT,
            FOREIGN KEY (listing_id) REFERENCES rental_listings (id)
        )
        """)
    
        self.cur.execute("""
        CREATE TABLE IF NOT EXISTS schools_within_catchment(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            listing_id INTEGER,
            school_id INTEGER,
            FOREIGN KEY(listing_id) REFERENCES rental_listings(id),
            FOREIGN KEY(school_id) REFERENCES schools(school_id)
        )
        """)

    def insert_listing2(self, listing):
        listing = list(listing)
        current_time = datetime.datetime.now().strftime('%Y-%m-%d')
        
        
        utilities = listing.pop(32) # utilities_included is at index 31/32?
        print(utilities)
        # Prepare the data to insert into the rental_listings table
        listing_data = listing + [current_time] + [True]
        
        #listing_data = tuple(listing_data)
        
        try:
            
            self.cur.execute("""
            INSERT INTO rental_listings VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, tuple(listing_data))
            
            utilities = utilities if utilities not in [None, np.nan] else []
            #print(f'id: {listing_data[0]}, Data being inserted: {utilities}, type: {type(utilities)}')
            for utility in utilities:
                
                self.cur.execute("""
                INSERT INTO utilities_included (listing_id, utility) VALUES (?, ?)
                """, (listing[1], utility)) #listing[1] is the foreign key 
                        
            self.insert_price_history(listing_data[1], listing_data[22], current_time)
            self.insert_schools_within_catchment(listing[1], listing[15],listing[16])
            
            self.con.commit()
        except sqlite3.IntegrityError:
            
            return f"Failed at id: {listing_data[0]}"
    
    def insert_listing(self, listing):
        listing = list(listing)
        current_time = datetime.datetime.now().strftime('%Y-%m-%d')
        
        utilities = listing.pop(32) # utilities_included is at index 31/32?
        utilities = utilities if utilities not in [None, np.nan] else []
        
        # Prepare the data to insert into the rental_listings table
        listing_data = listing + [current_time] + [True]
        
        utilities_data = [(listing[1], utility) for utility in utilities]

        try:
            self.con.execute('BEGIN TRANSACTION')
            
            self.cur.execute("""
            INSERT INTO rental_listings VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, tuple(listing_data))

            self.cur.executemany("""
            INSERT INTO utilities_included (listing_id, utility) VALUES (?, ?)
            """, utilities_data)

            self.insert_price_history(listing_data[1], listing_data[22], current_time)
            self.insert_schools_within_catchment(listing[1], listing[15],listing[16])

            self.con.execute('COMMIT')
        except sqlite3.IntegrityError as e:
            self.con.execute('ROLLBACK')
            print(f"Failed at id: {listing_data[0]}")
            print(f"Error: {e}")
   
    def insert_schools_within_catchment(self, listing_id, listing_lat, listing_long):
        school_db = School_db()
        insert_records = []
        all_attendance_areas = school_db.read_all_attendance_areas()
        
        for school_id in school_db.get_all_school_ids():
            polygon_dict = all_attendance_areas.get(school_id[0])
            if polygon_dict and polygon_module.check_user_in_polygons(polygon_dict, listing_lat, listing_long):
                # If the listing is in the catchment area, insert into schools_with_catchment
                print(f'Inserting Listing ID:{int(listing_id)} and school id: {school_id[0]}')
                insert_records.append((int(listing_id), school_id[0]))
                    
        self.cur.executemany("""INSERT INTO schools_within_catchment (listing_id, school_id) VALUES (?, ?)""", insert_records)
        #self.con.commit()

    def update_listing(self, listing_id, listing):
        current_time = datetime.datetime.now().strftime('%Y-%m-%d')
        listing_data = tuple(listing[1:]) + (current_time, True, listing_id)  # removed id from listing
        self.cur.execute("""
        UPDATE rental_listings
        SET ref_id = ?, userId = ?, phone = ?, phone_2 = ?, email = ?, availability = ?, a = ?, v = ?, f = ?, s = ?, title = ?, intro = ?, city = ?, community = ?, latitude = ?, longitude = ?, marker = ?, link = ?, thumb2 = ?, preferred_contact = ?, type = ?, price = ?, price2 = ?, beds = ?, beds2 = ?, sq_feet = ?, sq_feet2 = ?, baths = ?, cats = ?, dogs = ?, utilities_included = ?, baths2 = ?, last_update = ?, is_active = ?
        WHERE id = ?
        """, listing_data)
        self.update_price_history(listing_id, listing[22], current_time)
        self.con.commit()

    def deactivate_old_listings(self):
        current_time = datetime.datetime.now().strftime('%Y-%m-%d')
        self.cur.execute("""
        UPDATE rental_listings
        SET is_active = ?
        WHERE last_update < ?
        """, (False, current_time))
        self.con.commit()

    def fetch_data_by_key(self, table, key_column, key_value):
        self.cur.execute(f"SELECT * FROM {table} WHERE {key_column} = ?", (key_value,))
        row = self.cur.fetchone()
        return row
    
    def insert_price_history(self, listing_id, price, current_time):
        self.cur.execute("""
        INSERT INTO price_history (listing_id, price, date) VALUES (?, ?, ?)
        """, (listing_id, price, current_time))
        self.con.commit()

    def update_price_history(self, listing_id, new_price, current_time):
        self.cur.execute("""
        SELECT price FROM price_history WHERE listing_id = ? ORDER BY date DESC LIMIT 1
        """, (listing_id,))
        row = self.cur.fetchone()

        if row is None or row[0] != new_price:
            self.insert_price_history(listing_id, new_price, current_time)
    
    def close_connection(self):
        self.con.close()
