import pandas as pd
import numpy as np
from geopy.geocoders import Nominatim
import re

# CONSTANTS
NUM_ROWS = 1000000
SAMPLE_SIZE = 41667
LIST_SIZE = 20
TIMEOUT_TIME = 20
# UPLOAD DATAFRAME & FIX THE COLUMN NAME
def upload_fix_df(df_name):
    df = pd.read_csv(df_name,nrows = NUM_ROWS)
    df.columns = ['medallion','hack_license','vendor_id', 'rate_code', 'store_and_fwd_flag', 'pickup_datetime', 'dropoff_datetime', 'passenger_count', 'trip_time_in_secs', 'trip_distance', 'pickup_longitude', 'pickup_latitude', 'dropoff_longitude', 'dropoff_latitude']
    df = df.sample(n = SAMPLE_SIZE)
    return df

# ADD THE PICKUP AS AN ARRAY OF COORDINATES (LAT,LONG)
def add_pickup(df):
    df['pickup_coordinates'] = df[['pickup_latitude','pickup_longitude']].values.tolist()
    return df
# SAME AS ABOVE BUT FOR DROPOFF
def add_dropoff(df):
    df['dropoff_coordinates'] = df[['dropoff_latitude','dropoff_longitude']].values.tolist()
    return df
# RETURN ONLY THE PICKUP/DROPOFF COORDINATES AS A PANDAS DATAFRAME
def filter_locations(df_name):
    df = upload_fix_df(df_name)
    df = add_pickup(df)
    df = add_dropoff(df)
    df_prime = df[['pickup_coordinates','dropoff_coordinates']]
    return df_prime

def get_pickup_list(df):
    return df['pickup_coordinates'].tolist()[:LIST_SIZE]

def get_dropoff_list(df):
    return df['dropoff_coordinates'].tolist()[:LIST_SIZE]

def get_location(latitude,longitude):
    latitude = str(latitude)
    longitude = str(longitude)
    full_coord = latitude + ',' + longitude
    geoLoc = Nominatim(user_agent='GetLoc', timeout=TIMEOUT_TIME)
    locname = geoLoc.reverse(full_coord)
    return locname.address

def borough_counter(pickup_list):
    bronx_counter = 0
    brooklyn_counter = 0
    manhattan_counter = 0
    queens_counter = 0
    si_counter = 0    
    # Define the regex pattern
    bronx_pattern = r'\b(?:bronx|the\sbronx)\b'
    brooklyn_pattern = r'\b(?:brooklyn|the\sbrooklyn)\b'
    manhattan_pattern = r'\b(?:manhattan|the\smanhattan)\b'
    queens_pattern = r'\b(?:queens|the\squeens)\b'
    si_pattern = r'\b(?:staten\sisland|the\sstaten\sisland)\b'
    for i in range(len(pickup_list)):
        latitude = pickup_list[i][0]
        longitude = pickup_list[i][1]
        location = get_location(latitude,longitude)
        print(location)
    

        # Define the string to search (example)
        text = location

        # Use re.search() to check if the regex appears in the string
        bronx_match = re.search(bronx_pattern, text, re.IGNORECASE)
        brooklyn_match = re.search(brooklyn_pattern, text, re.IGNORECASE)
        manhattan_match = re.search(manhattan_pattern, text, re.IGNORECASE)
        queens_match = re.search(queens_pattern, text, re.IGNORECASE)
        si_match = re.search(si_pattern, text, re.IGNORECASE)

        if bronx_match:
            bronx_counter +=1
        elif brooklyn_match:
            brooklyn_counter +=1
        elif manhattan_match:
            manhattan_counter+=1
        elif queens_match:
            queens_counter+=1
        elif si_match:
            si_counter+=1

    return np.array([bronx_counter, brooklyn_counter, manhattan_counter, queens_counter, si_counter])

  
# TEST

# Jan 2010
taxi2010JanLoc = filter_locations('trip_data_2010_1.csv')
taxi2010JanPickup = get_pickup_list(taxi2010JanLoc)
jan_2010_borough_tracker = borough_counter(taxi2010JanPickup)
print(jan_2010_borough_tracker)
# Nov 2010
taxi2010NovLoc = filter_locations('trip_data_2010_11.csv')
taxi2010NovPickup = get_pickup_list(taxi2010NovLoc)
nov_2010_borough_tracker = borough_counter(taxi2010NovPickup)
print(nov_2010_borough_tracker)
# Dec 2010
taxi2010DecLoc = filter_locations('trip_data_2010_12.csv')
taxi2010DecPickup = get_pickup_list(taxi2010DecLoc)
dec_2010_borough_tracker = borough_counter(taxi2010DecPickup)
print(dec_2010_borough_tracker)
'''
# Jan 2011
taxi2011JanLoc = filter_locations('trip_data_2011_1.csv')
taxi2011JanPickup = get_pickup_list(taxi2011JanLoc)
jan_2011_borough_tracker = borough_counter(taxi2011JanPickup)
print(jan_2011_borough_tracker)
# Feb 2011
taxi2011FebLoc = filter_locations('trip_data_2011_2.csv')
taxi2011FebPickup = get_pickup_list(taxi2011FebLoc)
feb_2011_borough_tracker = borough_counter(taxi2011FebPickup)
print(feb_2011_borough_tracker)
'''



total_tracker = jan_2010_borough_tracker + nov_2010_borough_tracker + dec_2010_borough_tracker # + jan_2011_borough_tracker + feb_2011_borough_tracker

print(total_tracker)

# MAKE A NEW PANDAS DATA FRAME

data = {
        'Borough': ['The Bronx', 'Manhattan', 'Queens', 'Brooklyn', 'Staten Island'],
        'Number of Taxis': [total_tracker[0], total_tracker[2], total_tracker[3], total_tracker[1], total_tracker[4]]

}

result_df = pd.DataFrame(data)
print('Initialized DataFrame:')
print(result_df)




    

# taxi2010JanDropoff = get_dropoff_list(taxi2010JanLoc)


'''
taxi2010FebLoc = filter_locations('trip_data_2.csv')
taxi2010MarLoc = filter_locations('trip_data_3.csv')
taxi2010AprLoc = filter_locations('trip_data_4.csv')
taxi2010MayLoc = filter_locations('trip_data_5.csv')
taxi2010JunLoc = filter_locations('trip_data_6.csv')
taxi2010JulLoc= filter_locations('trip_data_7.csv')
taxi2010AugLoc = filter_locations('trip_data_8.csv')
taxi2010SepLoc = filter_locations('trip_data_9.csv')
taxi2010OctLoc = filter_locations('trip_data_10.csv')

taxi2010DecLoc = filter_locations('trip_data_12.csv')
    

print(taxi2010FebLoc)
print(taxi2010NovLoc)
print(taxi2010DecLoc)

taxi2010Jan = upload_fix_df('trip_data_1.csv')
print(taxi2010Jan)
taxi2010Jan = add_pickup(taxi2010Jan)
print(taxi2010Jan)
'''





