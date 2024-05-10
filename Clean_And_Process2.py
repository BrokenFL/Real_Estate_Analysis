import pandas as pd
import logging
import argparse
from Data_Loader import DataLoader

# Enhanced logging configuration
logging.basicConfig(filename='clean_and_process.log', level=logging.DEBUG, filemode='w',
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

class CleaningScriptError(Exception):
    """Custom exception class for cleaning script errors."""
    pass

def get_data_dictionary():
    """
    This dictionary aligns with the database schema and ensures that all data types are correct.
    """
    return {
        'listing_number': 'str',
        'cumulative_dom': 'int',
        'days_on_market': 'int',
        'short_address': 'str',
        'baths_full': 'int',
        'baths_half': 'int',
        'cancel_date': 'datetime64[ns]',
        'listing_date': 'datetime64[ns]',
        'list_price': 'float',
        'front_exp': 'str',
        'expiration_date': 'datetime64[ns]',
        'parcel_id': 'str',
        'sold_date': 'datetime64[ns]',
        'sold_price': 'float',
        'sqft_living': 'int',
        'sqft_total': 'int',
        'under_contract_date': 'datetime64[ns]',
        'waterfront': 'bool',
        'withdrawn_date': 'datetime64[ns]',
        'year_built': 'int',
        'year_roof_installed': 'int',
        'sqft_guest_house': 'int',
        'construction_cbs': 'bool',
        'storm_protection_accordion_shutters': 'str',
        'storm_protection_impact_glass': 'str',
        'storm_protection_panel_shutters': 'str',
        'public_remarks': 'str',
        'area': 'str',
        'temp_off_market_date': 'datetime64[ns]',
        'garage_spaces': 'int',
        'furnished_furnished': 'bool',
        'subdivision': 'str',
        'terms_of_sale': 'str',
        'zip_code': 'str',
        'city': 'str',
        'guest_house': 'bool',
        'high_school': 'str',
        'homeowners_assoc': 'bool',
        'lot_sqft': 'int',
        'private_pool': 'bool',
        'sold_price_sqft': 'float',
        'legal_desc': 'str',
        'spa': 'bool',
        'state_province': 'str',
        'street_number': 'int',
        'tax_year': 'int',
        'taxes': 'float',
        'total_bedrooms': 'int',
        'total_floors_stories': 'int',
        'total_units_in_bldg': 'int',
        'ttl_units_in_complex': 'int',
        'original_list_price': 'float',
        'development_name': 'str',
        'unit_floor_number': 'int',
        'unit_number': 'str',
        'type': 'str',
        'geo_area': 'str',
        'geo_lat': 'float',
        'geo_lon': 'float',
        'hoa_poa_coa_monthly': 'float',
        'special_assessment': 'bool',
        'parcel_subdivision': 'str',
        'event_date': 'datetime64[ns]',
        'end_of_listing_date': 'datetime64[ns]'
    }

def normalize_column_names(df):
    """
    Standardizes column names to ensure consistency in the database schema.
    """
    df.columns = (
        df.columns.str.replace(' ', '_')
                   .str.replace('-', '_')
                   .str.replace('/', '_')
                   .str.replace('#', 'number')
                   .str.replace(':', '')
                   .str.replace('(', '')
                   .str.replace(')', '')
                   .str.replace('___', '_')
                   .str.replace('__', '_')
                   .str.lower()
    )
    return df

def assign_data_types(df, data_dict):
    """
    Converts columns in the dataframe `df` to the types specified in `data_dict`.
    """
    for column, dtype in data_dict.items():
        if column in df.columns:
            if 'datetime' in dtype:
                df[column] = pd.to_datetime(df[column], errors='coerce')
            elif dtype == 'str':
                df[column] = df[column].astype(str)
            elif dtype == 'int':
                df[column] = pd.to_numeric(df[column], errors='coerce').fillna(0).astype(int)
            elif dtype == 'float':
                df[column] = pd.to_numeric(df[column
                                              df[column] = pd.to_numeric(df[column], errors='coerce')
            elif dtype == 'bool':
                df[column] = df[column].apply(lambda x: True if x == 'True' else False if x == 'False' else None)
    return df

def handle_booleans(df, data_dictionary):
    """
    Converts specific columns to booleans based on the mapping of true and false values.
    """
    boolean_columns = {
        'waterfront': {'true_values': ['Yes'], 'false_values': ['No']},
        'construction_cbs': {'true_values': ['Yes'], 'false_values': ['No']},
        'furnished_furnished': {'true_values': ['Yes'], 'false_values': ['No']},
        'guest_house': {'true_values': ['Yes'], 'false_values': ['No']},
        'homeowners_assoc': {'true_values': ['Yes', 'M', 'V'], 'false_values': ['No', 'N']},
        'private_pool': {'true_values': ['Yes'], 'false_values': ['No']},
        'spa': {'true_values': ['Yes'], 'false_values': ['No']},
        'special_assessment': {'true_values': ['Yes', 'Y'], 'false_values': ['No', 'N']}
    }

    def convert_to_boolean(df, column, true_values, false_values):
        df[column] = df[column].apply(lambda x: True if x in true_values else False if x in false_values else None)
        return df

    for column, params in boolean_columns.items():
        if column in df.columns:
            df = convert_to_boolean(df, column, params['true_values'], params['false_values'])
    return df

def handle_datetimes(df, data_dictionary):
    """
    Ensures that any columns that are supposed to be datetime objects are converted correctly.
    """
    for field, dtype in data_dictionary.items():
        if dtype == 'datetime64[ns]' and field in df.columns:
            df[field] = pd.to_datetime(df[field], errors='coerce')
    return df

def process_string_fields(df, data_dictionary):
    """
    Processes string fields, potentially cleaning them or setting them to a consistent format.
    """
    def clean_text_column(column):
        column = column.str.lower()  # Basic lowercase transformation
        column = column.str.replace('-', '')  # Remove dashes

        # Remove spaces for 'parcel_id' only, and preserve leading zeros
        if column.name == 'parcel_id':
            column = column.str.replace(' ', '')
            column = column.str.zfill(17)  # Ensure 17 characters with leading zeros

        return column

    for col, field_type in data_dictionary.items():
        if col in df.columns:  # Check if the column exists
            if field_type == 'str':
                df[col] = clean_text_column(df[col])
    return df

def handle_missing_values(df, data_dictionary):
    """
    Fills missing values based on specific rules for each column.
    """
    # Fill numeric columns with 0 where applicable
    numeric_columns = ['baths_half', 'garage_spaces', 'lot_sqft', 'sqft_guest_house', 'total_floors_stories', 'total_units_in_bldg', 'unit_floor_number', 'days_on_market', 'cumulative_dom']
    for col in numeric_columns:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)

    # Fill datetime columns with NaT where applicable
    datetime_columns = ['cancel_date', 'listing_date', 'sold_date', 'under_contract_date', 'withdrawn_date', 'expiration_date', 'temp_off_market_date', 'event_date', 'end_of_listing_date']
    for col in datetime_columns:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors='coerce')

    # Specific rules
    df['year_roof_installed'] = df['year_roof_installed'].fillna(df['year_built'])
    df['days_on_market'] = df['days_on_market'].fillna(df['cumulative_dom'])

    return df

def add_additional_columns(df):
    """
    Adds additional derived columns like `parcel_subdivision`, `event_date`, and `end_of_listing_date`.
    """
    if 'parcel_id' in df.columns:
        df['parcel_subdivision'] = df['parcel_id'].str[:10]

    # Compute `event_date` as the earliest of several date columns
    event_date_columns = ['sold_date', 'cancel_date', 'withdrawn_date', 'expiration_date', 'temp_off_market_date']
    df['event_date'] = pd.NaT
    for col in event_date_columns:
        if col in df.columns:
            df['event_date'] = df['event_date'].fillna(df[col])

    # Compute `end_of_listing_date`
    if 'listing_date' in df.columns and 'cumulative_dom' in df.columns:
        df['end_of_listing_date'] = pd.to_datetime(df['listing_date']) + pd.to_timedelta(df['cumulative_dom'], unit='D')
    return df

def normalize_subdivision_vectorized(df):
    """
    Normalizes the `subdivision` field by grouping and finding the most popular subdivision name or deriving it from `legal_desc`.
    """
    def get_first_two_words(text):
        if pd.isna(text):
            return None
        return ' '.join(text.split()[:2])

    if 'parcel_subdivision' in df.columns and 'subdivision' in df.columns:
        # Group listings by "parcel_subdivision"
        grouped = df.groupby('parcel_subdivision')

        # Find most popular subdivision (mode) for each group
        most_popular_subdivision = grouped['subdivision'].agg(lambda x: pd.Series.mode(x).iloc[0] if not pd.Series.mode(x).empty else None)

        # Update the 'subdivision' column using the most popular subdivision
        df['subdivision'] = df['parcel_subdivision'].map(most_popular_subdivision)

        # Handle missing values (using vectorized operations)
        mask = df['subdivision'].isna()
        df.loc[mask, 'subdivision'] = df.loc[mask, 'legal_desc'].apply(get_first_two_words)

    return df

def process_and_load_data(filepaths, db_name, create_new_db=False, table_name='properties'):
    """
    Processes each file and loads its data into the database.
    """
    data_loader = DataLoader(db_name)
    if create_new_db:
        logging.info("Creating new database.")
        data_loader.create_database()  # Ensure database exists

    for filepath in filepaths:
        try:
            logging.info(f"Starting processing for file: {filepath}")
            df = pd.read_csv(filepath)
            data_dict = get_data_dictionary()
            df = normalize_column_names(df)
            df = assign_data_types(df, data_dict)
            df = handle_booleans(df, data_dict)
            df = handle_datetimes(df, data_dict)
            df = process_string_fields(df, data_dict)
            df = handle_missing_values(df, data_dict)
            df = add_additional_columns(df)
            df = normalize_subdivision_vectorized(df)

            # Validate DataFrame before loading
            data_loader.validate_dataframe_schema(df, table_name, data_loader.get_full_schema_definitions())
            data_loader.insert_data(df, table_name)
            logging.info(f"Data successfully loaded for file: {filepath}")

        except Exception as e:
            logging.error(f"Error processing file {filepath}: {str(e)}")
            raise CleaningScriptError(f"Error processing file {filepath}: {str(e)}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Process and load data into SQL database.")
    parser.add_argument("filepaths", nargs="+", help="File paths of the CSV files to process")
    parser.add_argument("db_filename", help="Database file path")
    parser.add_argument("--create_new_db", action='store_true', help="Flag to create a new database if needed")
    args = parser.parse_args()

    process_and_load_data(args.filepaths, args.db_filename, args.create_new_db)