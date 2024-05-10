import sqlite3
import pandas as pd
import logging

# Configure logging
logging.basicConfig(level=logging.DEBUG, filename='data_loader.log', filemode='w',
                    format='%(asctime)s - %(levelname)s - %(name)s - %(message)s')

class DataLoader:
    def __init__(self, db_file):
        self.db_file = db_file
        logging.info("DataLoader initialized with database file: %s", db_file)

    def create_connection(self):
        """Create and return a database connection."""
        try:
            conn = sqlite3.connect(self.db_file)
            logging.info("Database connection successfully created.")
            return conn
        except Exception as e:
            logging.error("Failed to create database connection: %s", e)
            return None

    def close_connection(self, conn):
        """Safely close the database connection."""
        try:
            if conn:
                conn.close()
                logging.info("Database connection safely closed.")
        except Exception as e:
            logging.error("Failed to close database connection: %s", e)

    def create_database(self):
        """Create the database and initialize tables with predefined schemas."""
        conn = self.create_connection()
        if conn is not None:
            try:
                conn.executescript('''
                CREATE TABLE IF NOT EXISTS properties (
                    listing_number TEXT PRIMARY KEY,
                    type TEXT,
                    parcel_id TEXT,
                    short_address TEXT,
                    sqft_living INTEGER,
                    sqft_total INTEGER,
                    year_built INTEGER,
                    lot_sqft INTEGER,
                    total_bedrooms INTEGER,
                    total_floors_stories INTEGER
                );
                CREATE TABLE IF NOT EXISTS listing_details (
                    listing_number TEXT PRIMARY KEY,
                    cumulative_dom INTEGER,
                    days_on_market INTEGER,
                    listing_date DATETIME,
                    list_price REAL,
                    original_list_price REAL,
                    sold_price REAL,
                    sold_date DATETIME,
                    under_contract_date DATETIME,
                    expiration_date DATETIME,
                    cancel_date DATETIME,
                    withdrawn_date DATETIME,
                    temp_off_market_date DATETIME,
                    end_of_listing_date DATETIME,
                    event_date DATETIME,
                    terms_of_sale TEXT
                );
                CREATE TABLE IF NOT EXISTS property_features (
                    listing_number TEXT PRIMARY KEY,
                    baths_full INTEGER,
                    baths_half INTEGER,
                    garage_spaces INTEGER,
                    guest_house BOOLEAN,
                    private_pool BOOLEAN,
                    spa BOOLEAN,
                    waterfront BOOLEAN,
                    construction_cbs BOOLEAN,
                    storm_protection_accordion_shutters BOOLEAN,
                    storm_protection_impact_glass BOOLEAN,
                    storm_protection_panel_shutters BOOLEAN,
                    furnished_furnished BOOLEAN,
                    homeowners_assoc BOOLEAN
                );
                CREATE TABLE IF NOT EXISTS location (
                    listing_number TEXT PRIMARY KEY,
                    geo_lat REAL,
                    geo_lon REAL,
                    geo_area TEXT,
                    city TEXT,
                    state_province TEXT,
                    zip_code TEXT,
                    area TEXT,
                    subdivision TEXT,
                    parcel_subdivision TEXT,
                    development_name TEXT,
                    high_school TEXT
                );
                CREATE TABLE IF NOT EXISTS taxes_and_fees (
                    listing_number TEXT PRIMARY KEY,
                    tax_year INTEGER,
                    taxes REAL,
                    hoa_poa_coa_monthly REAL,
                    special_assessment REAL
                );
                CREATE TABLE IF NOT EXISTS unit_details (
                    listing_number TEXT PRIMARY KEY,
                    unit_number TEXT,
                    unit_floor_number INTEGER,
                    total_units_in_bldg INTEGER,
                    ttl_units_in_complex INTEGER
                );
                ''')
                logging.info("Database and tables created successfully.")
            except Exception as e:
                logging.error(f"An error occurred creating the database: {e}")
            finally:
                self.close_connection(conn)

    def execute_query(self, query, params=None, commit=False):
        """Execute a SQL query directly."""
        conn = self.create_connection()
        if conn is not None:
            try:
                cursor = conn.cursor()
                if params:
                    cursor.execute(query, params)
                else:
                    cursor.execute(query)
                if commit:
                    conn.commit()
                    logging.info("Query executed and changes committed.")
                return cursor
            except Exception as e:
                logging.error(f"An error occurred: {e}")
                return None
            finally:
                self.close_connection(conn)

    def insert_data(self, df, table_name):
        """Insert cleaned data into the specified table."""
        conn = self.create_connection()
        if conn is not None:
            try:
                df.to_sql(table_name, conn, if_exists='append', index=False)
                logging.info(f"Data inserted successfully into {table_name}.")
            except Exception as e:
                logging.error(f"An error occurred inserting data into {table_name}: {e}")
            finally:
                self.close_connection(conn)

    def update_data(self, df, table_name, condition):
        """Update data in the specified table based on a condition."""
        conn = self.create_connection()
        if conn is not None:
            try:
                set_clause = ', '.join([f"{col} = ?" for col in df.columns])
                sql = f"UPDATE {table_name} SET {set_clause} WHERE {condition}"
                for _, row in df.iterrows():
                    conn.execute(sql, tuple(row))
                conn.commit()
                logging.info(f"Data updated successfully in {table_name}.")
            except Exception as e:
                logging.error(f"An error occurred updating data in {table_name}: {e}")
            finally:
                self.close_connection(conn)

    def get_full_schema_definitions(self):
        """Get the full schema definitions for the database tables."""
        return {
            'properties': {
                'listing_number': str, 'type': str, 'parcel_id': str, 'short_address': str,
                'sqft_living': int, 'sqft_total': int, 'year_built': int, 'lot_sqft': int,
                'total_bedrooms': int, 'total_floors_stories': int
            },
            'listing_details': {
                'listing_number': str, 'cumulative_dom': int, 'days_on_market': int,
                'listing_date': 'datetime', 'list_price': float, 'original_list_price': float,
                'sold_price': float, 'sold_date': 'datetime', 'under_contract_date': 'datetime',
                'expiration_date': 'datetime', 'cancel_date': 'datetime', 'withdrawn_date': 'datetime',
                'temp_off_market_date': 'datetime', 'end_of_listing_date': 'datetime', 'event_date': 'datetime',
                'terms_of_sale': str
            },
            'property_features': {
                'listing_number': str, 'baths_full': int, 'baths_half': int, 'garage_spaces': int,
                'guest_house': bool, 'private_pool': bool, 'spa': bool, 'waterfront': bool,
                'construction_cbs': bool, 'storm_protection_accordion_shutters': bool,
                'storm_protection_impact_glass': bool, 'storm_protection_panel_shutters': bool,
                'furnished_furnished': bool, 'homeowners_assoc': bool
            },
            'location': {
                'listing_number': str, 'geo_lat': float, 'geo_lon': float, 'geo_area': str,
                'city': str, 'state_province': str, 'zip_code': str, 'area': str, 'subdivision': str,
                'parcel_subdivision': str, 'development_name': str, 'high_school': str
            },
            'taxes_and_fees': {
                'listing_number': str, 'tax_year': int, 'taxes': float, 'hoa_poa_coa_monthly': float, 'special_assessment': float
            },
            'unit_details': {
                'listing_number': str, 'unit_number': str, 'unit_floor_number': int, 'total_units_in_bldg': int, 'ttl_units_in_complex': int
            }
        }

    def validate_dataframe_schema(self, df, table_name, schema_definitions):
        """Validate a DataFrame against the schema of a table."""
        required_schema = schema_definitions.get(table_name)
        if not required_schema:
            raise ValueError(f"No schema defined for table: {table_name}")

        missing_columns = [col for col in required_schema if col not in df.columns]
        if missing_columns:
            raise ValueError(f"Missing columns in DataFrame for table {table_name}: {missing_columns}")

    def export_data_to_sql(self, df, table_name, columns):
        """Export data to SQL, replacing current contents."""
        conn = self.create_connection()
        if conn is not None:
            try:
                df[columns].to_sql(table_name, conn, if_exists='replace', index=False)
                logging.info(f"Data exported to {table_name} successfully.")
            except Exception as e:
                logging.error(f"An error occurred exporting data to {table_name}: {e}")
            finally:
                self.close_connection(conn)

    def execute_read_query(self, query, params=None):
        """Execute a SQL read query and return the results as a DataFrame."""
        conn = self.create_connection()
        if conn is None:
            logging.error("Failed to establish connection for query execution.")
            return pd.DataFrame()
        try:
            df = pd.read_sql_query(query, conn, params=params)
            return df
        except Exception as e:
            logging.error(f"An error occurred during query execution: {e}")
            return pd.DataFrame()
        finally:
            self.close_connection(conn)

    def fetch_unique_values(self, table, column):
        """Fetch unique values from a specified column in a specified table for UI dropdown."""
        query = f"SELECT DISTINCT {column} FROM {table}"
        return self.execute_read_query(query)[column].tolist()

    def fetch_data(self, query, params=None):
        """Fetch data based on a SQL query and parameters."""
        return self.execute_read_query(query, params)

    def fetch_filtered_data(self, params):
        """Fetch filtered data based on user selections including date range."""
        field_map = {
            "New Listings": [("listing_details", "listing_date")],
            "Closed Listings": [("listing_details", "sold_date")],
            "Avg Price Per Ft": [("listing_details", "sold_date"), ("properties", "sqft_living"), ("listing_details", "sold_price")],
            "Avg Days on Market": [("listing_details", "sold_date"), ("listing_details", "cumulative_dom")],
            "Total Volume": [("listing_details", "sold_date"), ("listing_details", "sold_price")],
            "Pending Listings": [("listing_details", "listing_date"), ("listing_details", "under_contract_date"), ("listing_details", "end_of_listing_date")],
            "List to Sold Ratio": [("listing_details", "sold_date"), ("listing_details", "sold_price"), ("listing_details", "list_price")],
            "Active Inventory": [("listing_details", "listing_date"), ("listing_details", "under_contract_date"), ("listing_details", "end_of_listing_date")],
            "Month Supply of Inventory": [("listing_details", "listing_date"), ("listing_details", "sold_date")],
            "% of Cash Sales": [("listing_details", "sold_date"), ("listing_details", "terms_of_sale")]
        }
        conditions = []
        values = []

        if params.get('city') and params['city'] != "All":
            conditions.append("city = ?")
            values.append(params['city'])

        if params.get('subdivision') and params['subdivision'] != "All":
            conditions.append("subdivision = ?")
            values.append(params['subdivision'])

        if params.get('building_type') and params['building_type'] != "All":
            conditions.append("type = ?")
            values.append(params['building_type'])

        if params.get('start_date'):
            conditions.append("listing_date >= ?")
            values.append(params['start_date'])

        if params.get('end_date'):
            conditions.append("listing_date <= ?")
            values.append(params['end_date'])

        where_clause = " AND ".join(conditions) if conditions else "1=1"

        query = f"""
        SELECT *
        FROM listing_details
        JOIN properties ON listing_details.listing_number = properties.listing_number
        JOIN location ON listing_details.listing_number = location.listing_number
        WHERE {where_clause}
        """

        return self.execute_read_query(query, values)

    def get_min_max_dates(self, table_name):
        """Fetch the earliest and latest dates from the specified table."""
        query = f"SELECT MIN(listing_date) AS min_date, MAX(listing_date) AS max_date FROM {table_name}"
        return self.execute_read_query(query)

    def batch_insert_data(self, df, table_name, batch_size=1000):
        """Insert data in batches to manage large datasets efficiently."""
        conn = self.create_connection()
        if conn is not None:
            try:
                cursor = conn.cursor()
                for start in range(0, len(df), batch_size):
                    end = start + batch_size
                    batch_data = df.iloc[start:end]
                    batch_data.to_sql(table_name, conn, if_exists='append', index=False)
                conn.commit()
                logging.info(f"Batch data inserted successfully into {table_name}.")
            except Exception as e:
                logging.error(f"An error occurred during batch data insertion into {table_name}: {e}")
            finally:
                self.close_connection(conn)

    def update_multiple_data(self, updates, table_name):
        """Update multiple records in a single transaction."""
        conn = self.create_connection()
        if conn is not None:
            try:
                cursor = conn.cursor()
                for update in updates:
                    sql, params = update
                    cursor.execute(sql, params)
                conn.commit()
                logging.info(f"Multiple records updated successfully in {table_name}.")
            except Exception as e:
                logging.error(f"An error occurred updating multiple records in {table_name}: {e}")
            finally:
                self.close_connection(conn)

    def delete_data(self, table_name, condition, params):
        """Delete data from a table based on a condition."""
        conn = self.create_connection()
        if conn is not None:
            try:
                sql = f"DELETE FROM {table_name} WHERE {condition}"
                conn.execute(sql, params)
                conn.commit()
                logging.info(f"Data deleted successfully from {table_name} based on condition: {condition}.")
            except Exception as e:
                logging.error(f"An error occurred deleting data from {table_name}: {e}")
            finally:
                self.close_connection(conn)

    def get_unique_building_types(self):
        """Fetch unique building types for UI dropdown."""
        return self.fetch_unique_values('properties', 'type')

# Example usage
if __name__ == "__main__":
    db_path = 'path_to_your_database.db'
    data_loader = DataLoader(db_path)

    # Create the database with tables
    data_loader.create_database()

    # Example DataFrames prepared for each table
    df_properties = pd.DataFrame({
        'listing_number': ['LN001', 'LN002'],
        'type': ['Condo', 'House'],
        'parcel_id': ['P001', 'P002'],
        'short_address': ['1234 Elm St', '5678 Maple Ave'],
        'sqft_living': [1200, 2500],
        'sqft_total': [1300, 2600],
        'year_built': [2000, 1990],
        'lot_sqft': [5000, 10000],
        'total_bedrooms': [3, 4],
        'total_floors_stories': [2, 2]
    })

    # Insert data into the 'properties' table
    data_loader.insert_data(df_properties, 'properties')

    # Fetch unique cities and building types
    print("Unique building types:", data_loader.get_unique_building_types())

    # Fetch filtered data based on parameters
    params = {
        'city': 'Example City',
        'subdivision': 'Example Subdivision',
        'building_type': 'Residential',
        'start_date': '2021-01-01',
        'end_date': '2021-12-31'
    }
    filtered_data = data_loader.fetch_filtered_data(params)
    print("Filtered Data:\n", filtered_data)

    # Get the minimum and maximum dates from the 'listing_details' table
    min_max_dates = data_loader.get_min_max_dates('listing_details')
    print("Min and Max Dates:", min_max_dates)