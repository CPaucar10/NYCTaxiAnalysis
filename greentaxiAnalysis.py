import pyarrow.parquet as pq
from datetime import datetime
import pandas as pd

# CONSTANTS
NUM_MONTHLY_TRIPS = 500
BRONX_ID = [200,220,240,241,136,235,119,247,168,199,126,213,208,58,184,46,51,81,259,254,174,18,94,169,69,159,147,212,250,183,3,32,31,20,47,167,60,248,182,242,185,78,59]
BROOKLYN_ID = [112,255,256,217,34,66,33,54,195,228,14,11,55,29,150,154,155,39,222,76,63,37,36,80,17,49,97,65,25,52,40,106,181,111,257,227,67,22,21,108,123,210,149,91,71,72,35,77,177,225,61,189,190,133,26,178,165,89,85,188,62]
MANHATTAN_ID = [128,243,244,116,152,166,24,151,238,239,143,142,50,48,246,68,158,249,125,211,231,13,261,12,88,104,103,105,87,209,45,144,148,232,114,113,79,4,224,137,233,229,141,140,202,263,262,75,74,194,42,120,127,153,41,43,236,237,163,230,161,162,107,234,90,186,164,170,100]
QUEENS_ID = [8,252, 223, 179,53, 33,138, 15, 193, 7, 207, 146, 10, 70, 129, 145, 260, 226, 83, 32, 171,253,92,16,173,57,73,64,9,82,56,93,192,27,157,175,101,196,98,135,121,19,160,95,131,198,102,28,134,191,96,122,130,197,258,215,205,38,180,216,218,124,2,30,201,132,139,219,203,86,117]
SI_ID = [206,187,156,23,99,5,204,44,84,109,110,176,172,214,6,221,245,251,118,115]
GREEN_TAXI = ['Green Taxi'] * NUM_MONTHLY_TRIPS
def parquet_to_df(pq_name):
    trips = pq.read_table(pq_name)
    trips = trips.to_pandas()
    trips = trips.head(NUM_MONTHLY_TRIPS)
    # trips = trips.sample(NUM_MONTHLY_TRIPS)
    return trips

def extract_data(df_name):
    new_df = df_name[['lpep_pickup_datetime','PULocationID']]
    return new_df
def add_borough(df_name):
    borough = []
    locationID = df_name['PULocationID'].tolist()
    for id in locationID:
        if id in BRONX_ID:
            borough.append('The Bronx')
        elif id in BROOKLYN_ID:
            borough.append('Brooklyn')
        elif id in MANHATTAN_ID:
            borough.append('Manhattan')
        elif id in QUEENS_ID:
            borough.append('Queens')
        else:
            borough.append('Staten Island')
            
    return borough

def add_to_df(df_name, column_name,  arr):
    df_name[column_name] = arr
    return df_name

def date_convert(date_str):
    datetime_obj = datetime.strptime(str(date_str), "%Y-%m-%d %H:%M:%S")
    month_year = datetime_obj.strftime("%B %Y")
    return month_year

def format_date(df):
    new_dates = []
    old_dates = df['lpep_pickup_datetime'].tolist()
    # print('Type is',type(old_dates[0]))
    for date in old_dates:
        new_date = date_convert(date)
        new_dates.append(new_date)
    df['lpep_pickup_datetime'] = new_dates
    df = df.rename(columns={'lpep_pickup_datetime': 'Date'})
    df = df.drop(columns=['PULocationID'])
    return df

def combine_dfs(*args):
    df_combined = pd.concat(args, axis=0)
    return df_combined



### 2015
    
## JAN 2015
jan_green_2015 = parquet_to_df('green_tripdata_2015-01.parquet')
jan_green_2015 = extract_data(jan_green_2015)
jan_green_2015_boroughs = add_borough(jan_green_2015)
jan_green_2015 = add_to_df(jan_green_2015, 'Borough', jan_green_2015_boroughs)
jan_green_2015 = format_date(jan_green_2015)
# jan_green_2015 = add_to_df(jan_green_2015, 'Type of Ride', GREEN_TAXI)
# green_taxi = ['Green Taxi'] * NUM_MONTHLY_TRIPS

# print(jan_green_2015)

## FEB 2015
feb_green_2015 = parquet_to_df('green_tripdata_2015-02.parquet')
feb_green_2015 = extract_data(feb_green_2015)
feb_green_2015_boroughs = add_borough(feb_green_2015)
feb_green_2015 = add_to_df(feb_green_2015, 'Borough', feb_green_2015_boroughs)
feb_green_2015 = format_date(feb_green_2015)
# print(feb_green_2015)



## MAR 2015
mar_green_2015 = parquet_to_df('green_tripdata_2015-03.parquet')
mar_green_2015 = extract_data(mar_green_2015)
mar_green_2015_boroughs = add_borough(mar_green_2015)
mar_green_2015 = add_to_df(mar_green_2015, 'Borough', mar_green_2015_boroughs)
mar_green_2015 = format_date(mar_green_2015)
# print(mar_green_2015)



## APR 2015
apr_green_2015 = parquet_to_df('green_tripdata_2015-04.parquet')
apr_green_2015 = extract_data(apr_green_2015)
apr_green_2015_boroughs = add_borough(apr_green_2015)
apr_green_2015 = add_to_df(apr_green_2015, 'Borough', apr_green_2015_boroughs)
apr_green_2015 = format_date(apr_green_2015)
# jan_green_2015 = add_to_df(jan_green_2015, 'Type of Ride', GREEN_TAXI)
# green_taxi = ['Green Taxi'] * NUM_MONTHLY_TRIPS

# print(apr_green_2015)

## MAY 2015
may_green_2015 = parquet_to_df('green_tripdata_2015-05.parquet')
may_green_2015 = extract_data(may_green_2015)
may_green_2015_boroughs = add_borough(may_green_2015)
may_green_2015 = add_to_df(may_green_2015, 'Borough', may_green_2015_boroughs)
may_green_2015 = format_date(may_green_2015)
# print(may_green_2015)

## JUN 2015
jun_green_2015 = parquet_to_df('green_tripdata_2015-06.parquet')
jun_green_2015 = extract_data(jun_green_2015)
jun_green_2015_boroughs = add_borough(jun_green_2015)
jun_green_2015 = add_to_df(jun_green_2015, 'Borough', jun_green_2015_boroughs)
jun_green_2015 = format_date(jun_green_2015)
# print(jun_green_2015)

## JUL 2015
jul_green_2015 = parquet_to_df('green_tripdata_2015-07.parquet')
jul_green_2015 = extract_data(jul_green_2015)
jul_green_2015_boroughs = add_borough(jul_green_2015)
jul_green_2015 = add_to_df(jul_green_2015, 'Borough', jul_green_2015_boroughs)
jul_green_2015 = format_date(jul_green_2015)
# jan_green_2015 = add_to_df(jan_green_2015, 'Type of Ride', GREEN_TAXI)
# green_taxi = ['Green Taxi'] * NUM_MONTHLY_TRIPS

# print(jul_green_2015)

## AUG 2015
aug_green_2015 = parquet_to_df('green_tripdata_2015-08.parquet')
aug_green_2015 = extract_data(aug_green_2015)
aug_green_2015_boroughs = add_borough(aug_green_2015)
aug_green_2015 = add_to_df(aug_green_2015, 'Borough', aug_green_2015_boroughs)
aug_green_2015 = format_date(aug_green_2015)
# print(aug_green_2015)

## SEP 2015
sep_green_2015 = parquet_to_df('green_tripdata_2015-09.parquet')
sep_green_2015 = extract_data(sep_green_2015)
sep_green_2015_boroughs = add_borough(sep_green_2015)
sep_green_2015 = add_to_df(sep_green_2015, 'Borough', sep_green_2015_boroughs)
sep_green_2015 = format_date(sep_green_2015)
# print(sep_green_2015)

## OCT 2015
oct_green_2015 = parquet_to_df('green_tripdata_2015-10.parquet')
oct_green_2015 = extract_data(oct_green_2015)
oct_green_2015_boroughs = add_borough(oct_green_2015)
oct_green_2015 = add_to_df(oct_green_2015, 'Borough', oct_green_2015_boroughs)
oct_green_2015 = format_date(oct_green_2015)
# jan_green_2015 = add_to_df(jan_green_2015, 'Type of Ride', GREEN_TAXI)
# green_taxi = ['Green Taxi'] * NUM_MONTHLY_TRIPS

# print(oct_green_2015)

## NOV 2015
nov_green_2015 = parquet_to_df('green_tripdata_2015-11.parquet')
nov_green_2015 = extract_data(nov_green_2015)
nov_green_2015_boroughs = add_borough(nov_green_2015)
nov_green_2015 = add_to_df(nov_green_2015, 'Borough', nov_green_2015_boroughs)
nov_green_2015 = format_date(nov_green_2015)
# print(nov_green_2015)

## DEC 2015
dec_green_2015 = parquet_to_df('green_tripdata_2015-12.parquet')
dec_green_2015 = extract_data(dec_green_2015)
dec_green_2015_boroughs = add_borough(dec_green_2015)
dec_green_2015 = add_to_df(dec_green_2015, 'Borough', dec_green_2015_boroughs)
dec_green_2015 = format_date(dec_green_2015)
# print(dec_green_2015)


### 2016

## JAN 2016
jan_green_2016= parquet_to_df('green_tripdata_2016-01.parquet')
jan_green_2016 = extract_data(jan_green_2016)
jan_green_2016_boroughs = add_borough(jan_green_2016)
jan_green_2016 = add_to_df(jan_green_2016, 'Borough', jan_green_2016_boroughs)
jan_green_2016 = format_date(jan_green_2016)
# jan_green_2015 = add_to_df(jan_green_2015, 'Type of Ride', GREEN_TAXI)
# green_taxi = ['Green Taxi'] * NUM_MONTHLY_TRIPS

# print(jan_green_2016)

## FEB 2016
feb_green_2016 = parquet_to_df('green_tripdata_2016-02.parquet')
feb_green_2016 = extract_data(feb_green_2016)
feb_green_2016_boroughs = add_borough(feb_green_2016)
feb_green_2016 = add_to_df(feb_green_2016, 'Borough', feb_green_2016_boroughs)
feb_green_2016 = format_date(feb_green_2016)
# print(feb_green_2016)

## MAR 2016
mar_green_2016 = parquet_to_df('green_tripdata_2016-03.parquet')
mar_green_2016 = extract_data(mar_green_2016)
mar_green_2016_boroughs = add_borough(mar_green_2016)
mar_green_2016 = add_to_df(mar_green_2016, 'Borough', mar_green_2016_boroughs)
mar_green_2016 = format_date(mar_green_2016)
# print(mar_green_2016)

## APR 2016
apr_green_2016 = parquet_to_df('green_tripdata_2016-04.parquet')
apr_green_2016 = extract_data(apr_green_2016)
apr_green_2016_boroughs = add_borough(apr_green_2016)
apr_green_2016 = add_to_df(apr_green_2016, 'Borough', apr_green_2016_boroughs)
apr_green_2016 = format_date(apr_green_2016)
# jan_green_2015 = add_to_df(jan_green_2015, 'Type of Ride', GREEN_TAXI)
# green_taxi = ['Green Taxi'] * NUM_MONTHLY_TRIPS

# print(apr_green_2016)

## MAY 2016
may_green_2016 = parquet_to_df('green_tripdata_2016-05.parquet')
may_green_2016 = extract_data(may_green_2016)
may_green_2016_boroughs = add_borough(may_green_2016)
may_green_2016 = add_to_df(may_green_2016, 'Borough', may_green_2016_boroughs)
may_green_2016 = format_date(may_green_2016)
# print(may_green_2016)

## JUN 2016
jun_green_2016 = parquet_to_df('green_tripdata_2016-06.parquet')
jun_green_2016 = extract_data(jun_green_2016)
jun_green_2016_boroughs = add_borough(jun_green_2016)
jun_green_2016 = add_to_df(jun_green_2016, 'Borough', jun_green_2016_boroughs)
jun_green_2016 = format_date(jun_green_2016)
# print(jun_green_2016)


## JUL 2016
jul_green_2016 = parquet_to_df('green_tripdata_2016-07.parquet')
jul_green_2016 = extract_data(jul_green_2016)
jul_green_2016_boroughs = add_borough(jul_green_2016)
jul_green_2016 = add_to_df(jul_green_2016, 'Borough', jul_green_2016_boroughs)
jul_green_2016 = format_date(jul_green_2016)
# jan_green_2016 = add_to_df(jan_green_2016, 'Type of Ride', GREEN_TAXI)
# green_taxi = ['Green Taxi'] * NUM_MONTHLY_TRIPS


# print(jul_green_2016)


## AUG 2016
aug_green_2016 = parquet_to_df('green_tripdata_2016-08.parquet')
aug_green_2016 = extract_data(aug_green_2016)
aug_green_2016_boroughs = add_borough(aug_green_2016)
aug_green_2016 = add_to_df(aug_green_2016, 'Borough', aug_green_2016_boroughs)
aug_green_2016 = format_date(aug_green_2016)
# print(aug_green_2016)


## SEP 2016
sep_green_2016 = parquet_to_df('green_tripdata_2016-09.parquet')
sep_green_2016 = extract_data(sep_green_2016)
sep_green_2016_boroughs = add_borough(sep_green_2016)
sep_green_2016 = add_to_df(sep_green_2016, 'Borough', sep_green_2016_boroughs)
sep_green_2016 = format_date(sep_green_2016)
# print(sep_green_2016)


## OCT 2016
oct_green_2016 = parquet_to_df('green_tripdata_2016-10.parquet')
oct_green_2016 = extract_data(oct_green_2016)
oct_green_2016_boroughs = add_borough(oct_green_2016)
oct_green_2016 = add_to_df(oct_green_2016, 'Borough', oct_green_2016_boroughs)
oct_green_2016 = format_date(oct_green_2016)
# jan_green_2016 = add_to_df(jan_green_2016, 'Type of Ride', GREEN_TAXI)
# green_taxi = ['Green Taxi'] * NUM_MONTHLY_TRIPS


# print(oct_green_2016)


## NOV 2016
nov_green_2016 = parquet_to_df('green_tripdata_2016-11.parquet')
nov_green_2016 = extract_data(nov_green_2016)
nov_green_2016_boroughs = add_borough(nov_green_2016)
nov_green_2016 = add_to_df(nov_green_2016, 'Borough', nov_green_2016_boroughs)
nov_green_2016 = format_date(nov_green_2016)
# print(nov_green_2016)


## DEC 2016
dec_green_2016 = parquet_to_df('green_tripdata_2016-12.parquet')
dec_green_2016 = extract_data(dec_green_2016)
dec_green_2016_boroughs = add_borough(dec_green_2016)
dec_green_2016 = add_to_df(dec_green_2016, 'Borough', dec_green_2016_boroughs)
dec_green_2016 = format_date(dec_green_2016)
# print(dec_green_2016)

## 2017

## JAN 2017
jan_green_2017= parquet_to_df('green_tripdata_2017-01.parquet')
jan_green_2017 = extract_data(jan_green_2017)
jan_green_2017_boroughs = add_borough(jan_green_2017)
jan_green_2017 = add_to_df(jan_green_2017, 'Borough', jan_green_2017_boroughs)
jan_green_2017 = format_date(jan_green_2017)
# jan_green_2015 = add_to_df(jan_green_2015, 'Type of Ride', GREEN_TAXI)
# green_taxi = ['Green Taxi'] * NUM_MONTHLY_TRIPS


# print(jan_green_2017)


## FEB 2017
feb_green_2017 = parquet_to_df('green_tripdata_2017-02.parquet')
feb_green_2017 = extract_data(feb_green_2017)
feb_green_2017_boroughs = add_borough(feb_green_2017)
feb_green_2017 = add_to_df(feb_green_2017, 'Borough', feb_green_2017_boroughs)
feb_green_2017 = format_date(feb_green_2017)
# print(feb_green_2017)


## MAR 2017
mar_green_2017 = parquet_to_df('green_tripdata_2017-03.parquet')
mar_green_2017 = extract_data(mar_green_2017)
mar_green_2017_boroughs = add_borough(mar_green_2017)
mar_green_2017 = add_to_df(mar_green_2017, 'Borough', mar_green_2017_boroughs)
mar_green_2017 = format_date(mar_green_2017)
# print(mar_green_2017)


## APR 2017
apr_green_2017 = parquet_to_df('green_tripdata_2017-04.parquet')
apr_green_2017 = extract_data(apr_green_2017)
apr_green_2017_boroughs = add_borough(apr_green_2017)
apr_green_2017 = add_to_df(apr_green_2017, 'Borough', apr_green_2017_boroughs)
apr_green_2017 = format_date(apr_green_2017)
# jan_green_2015 = add_to_df(jan_green_2015, 'Type of Ride', GREEN_TAXI)
# green_taxi = ['Green Taxi'] * NUM_MONTHLY_TRIPS


# print(apr_green_2017)


## MAY 2017
may_green_2017 = parquet_to_df('green_tripdata_2017-05.parquet')
may_green_2017 = extract_data(may_green_2017)
may_green_2017_boroughs = add_borough(may_green_2017)
may_green_2017 = add_to_df(may_green_2017, 'Borough', may_green_2017_boroughs)
may_green_2017 = format_date(may_green_2017)
# print(may_green_2017)


## JUN 2017
jun_green_2017 = parquet_to_df('green_tripdata_2017-07.parquet')
jun_green_2017 = extract_data(jun_green_2017)
jun_green_2017_boroughs = add_borough(jun_green_2017)
jun_green_2017 = add_to_df(jun_green_2017, 'Borough', jun_green_2017_boroughs)
jun_green_2017 = format_date(jun_green_2017)
# print(jun_green_2017)




## JUN 2017
jun_green_2017 = parquet_to_df('green_tripdata_2017-07.parquet')
jun_green_2017 = extract_data(jun_green_2017)
jun_green_2017_boroughs = add_borough(jun_green_2017)
jun_green_2017 = add_to_df(jun_green_2017, 'Borough', jun_green_2017_boroughs)
jun_green_2017 = format_date(jun_green_2017)
print(jun_green_2017)


## JUL 2017
jul_green_2017 = parquet_to_df('green_tripdata_2017-07.parquet')
jul_green_2017 = extract_data(jul_green_2017)
jul_green_2017_boroughs = add_borough(jul_green_2017)
jul_green_2017 = add_to_df(jul_green_2017, 'Borough', jul_green_2017_boroughs)
jul_green_2017 = format_date(jul_green_2017)
# jan_green_2017 = add_to_df(jan_green_2017, 'Type of Ride', GREEN_TAXI)
# green_taxi = ['Green Taxi'] * NUM_MONTHLY_TRIPS


print(jul_green_2017)


## AUG 2017
aug_green_2017 = parquet_to_df('green_tripdata_2017-08.parquet')
aug_green_2017 = extract_data(aug_green_2017)
aug_green_2017_boroughs = add_borough(aug_green_2017)
aug_green_2017 = add_to_df(aug_green_2017, 'Borough', aug_green_2017_boroughs)
aug_green_2017 = format_date(aug_green_2017)
print(aug_green_2017)


## SEP 2017
sep_green_2017 = parquet_to_df('green_tripdata_2017-09.parquet')
sep_green_2017 = extract_data(sep_green_2017)
sep_green_2017_boroughs = add_borough(sep_green_2017)
sep_green_2017 = add_to_df(sep_green_2017, 'Borough', sep_green_2017_boroughs)
sep_green_2017 = format_date(sep_green_2017)
print(sep_green_2017)


## OCT 2017
oct_green_2017 = parquet_to_df('green_tripdata_2017-10.parquet')
oct_green_2017 = extract_data(oct_green_2017)
oct_green_2017_boroughs = add_borough(oct_green_2017)
oct_green_2017 = add_to_df(oct_green_2017, 'Borough', oct_green_2017_boroughs)
oct_green_2017 = format_date(oct_green_2017)
# jan_green_2017 = add_to_df(jan_green_2017, 'Type of Ride', GREEN_TAXI)
# green_taxi = ['Green Taxi'] * NUM_MONTHLY_TRIPS


print(oct_green_2017)


## NOV 2017
nov_green_2017 = parquet_to_df('green_tripdata_2017-11.parquet')
nov_green_2017 = extract_data(nov_green_2017)
nov_green_2017_boroughs = add_borough(nov_green_2017)
nov_green_2017 = add_to_df(nov_green_2017, 'Borough', nov_green_2017_boroughs)
nov_green_2017 = format_date(nov_green_2017)
print(nov_green_2017)


## DEC 2017
dec_green_2017 = parquet_to_df('green_tripdata_2017-12.parquet')
dec_green_2017 = extract_data(dec_green_2017)
dec_green_2017_boroughs = add_borough(dec_green_2017)
dec_green_2017 = add_to_df(dec_green_2017, 'Borough', dec_green_2017_boroughs)
dec_green_2017 = format_date(dec_green_2017)
print(dec_green_2017)

### 2018
## JAN 2018
jan_green_2018= parquet_to_df('green_tripdata_2018-01.parquet')
jan_green_2018 = extract_data(jan_green_2018)
jan_green_2018_boroughs = add_borough(jan_green_2018)
jan_green_2018 = add_to_df(jan_green_2018, 'Borough', jan_green_2018_boroughs)
jan_green_2018 = format_date(jan_green_2018)
# jan_green_2015 = add_to_df(jan_green_2015, 'Type of Ride', GREEN_TAXI)
# green_taxi = ['Green Taxi'] * NUM_MONTHLY_TRIPS


# print(jan_green_2018)


## FEB 2018
feb_green_2018 = parquet_to_df('green_tripdata_2018-02.parquet')
feb_green_2018 = extract_data(feb_green_2018)
feb_green_2018_boroughs = add_borough(feb_green_2018)
feb_green_2018 = add_to_df(feb_green_2018, 'Borough', feb_green_2018_boroughs)
feb_green_2018 = format_date(feb_green_2018)
# print(feb_green_2018)


## MAR 2018
mar_green_2018 = parquet_to_df('green_tripdata_2018-03.parquet')
mar_green_2018 = extract_data(mar_green_2018)
mar_green_2018_boroughs = add_borough(mar_green_2018)
mar_green_2018 = add_to_df(mar_green_2018, 'Borough', mar_green_2018_boroughs)
mar_green_2018 = format_date(mar_green_2018)
# print(mar_green_2018)


## APR 2018
apr_green_2018 = parquet_to_df('green_tripdata_2018-04.parquet')
apr_green_2018 = extract_data(apr_green_2018)
apr_green_2018_boroughs = add_borough(apr_green_2018)
apr_green_2018 = add_to_df(apr_green_2018, 'Borough', apr_green_2018_boroughs)
apr_green_2018 = format_date(apr_green_2018)
# jan_green_2015 = add_to_df(jan_green_2015, 'Type of Ride', GREEN_TAXI)
# green_taxi = ['Green Taxi'] * NUM_MONTHLY_TRIPS


# print(apr_green_2018)


## MAY 2018
may_green_2018 = parquet_to_df('green_tripdata_2018-05.parquet')
may_green_2018 = extract_data(may_green_2018)
may_green_2018_boroughs = add_borough(may_green_2018)
may_green_2018 = add_to_df(may_green_2018, 'Borough', may_green_2018_boroughs)
may_green_2018 = format_date(may_green_2018)
# print(may_green_2018)


## JUN 2018
jun_green_2018 = parquet_to_df('green_tripdata_2018-08.parquet')
jun_green_2018 = extract_data(jun_green_2018)
jun_green_2018_boroughs = add_borough(jun_green_2018)
jun_green_2018 = add_to_df(jun_green_2018, 'Borough', jun_green_2018_boroughs)
jun_green_2018 = format_date(jun_green_2018)
# print(jun_green_2018)




## JUN 2018
jun_green_2018 = parquet_to_df('green_tripdata_2018-08.parquet')
jun_green_2018 = extract_data(jun_green_2018)
jun_green_2018_boroughs = add_borough(jun_green_2018)
jun_green_2018 = add_to_df(jun_green_2018, 'Borough', jun_green_2018_boroughs)
jun_green_2018 = format_date(jun_green_2018)
print(jun_green_2018)


## JUL 2018
jul_green_2018 = parquet_to_df('green_tripdata_2018-08.parquet')
jul_green_2018 = extract_data(jul_green_2018)
jul_green_2018_boroughs = add_borough(jul_green_2018)
jul_green_2018 = add_to_df(jul_green_2018, 'Borough', jul_green_2018_boroughs)
jul_green_2018 = format_date(jul_green_2018)
# jan_green_2018 = add_to_df(jan_green_2018, 'Type of Ride', GREEN_TAXI)
# green_taxi = ['Green Taxi'] * NUM_MONTHLY_TRIPS


print(jul_green_2018)


## AUG 2018
aug_green_2018 = parquet_to_df('green_tripdata_2018-08.parquet')
aug_green_2018 = extract_data(aug_green_2018)
aug_green_2018_boroughs = add_borough(aug_green_2018)
aug_green_2018 = add_to_df(aug_green_2018, 'Borough', aug_green_2018_boroughs)
aug_green_2018 = format_date(aug_green_2018)
print(aug_green_2018)


## SEP 2018
sep_green_2018 = parquet_to_df('green_tripdata_2018-09.parquet')
sep_green_2018 = extract_data(sep_green_2018)
sep_green_2018_boroughs = add_borough(sep_green_2018)
sep_green_2018 = add_to_df(sep_green_2018, 'Borough', sep_green_2018_boroughs)
sep_green_2018 = format_date(sep_green_2018)
print(sep_green_2018)


## OCT 2018
oct_green_2018 = parquet_to_df('green_tripdata_2018-10.parquet')
oct_green_2018 = extract_data(oct_green_2018)
oct_green_2018_boroughs = add_borough(oct_green_2018)
oct_green_2018 = add_to_df(oct_green_2018, 'Borough', oct_green_2018_boroughs)
oct_green_2018 = format_date(oct_green_2018)
# jan_green_2018 = add_to_df(jan_green_2018, 'Type of Ride', GREEN_TAXI)
# green_taxi = ['Green Taxi'] * NUM_MONTHLY_TRIPS


print(oct_green_2018)


## NOV 2018
nov_green_2018 = parquet_to_df('green_tripdata_2018-11.parquet')
nov_green_2018 = extract_data(nov_green_2018)
nov_green_2018_boroughs = add_borough(nov_green_2018)
nov_green_2018 = add_to_df(nov_green_2018, 'Borough', nov_green_2018_boroughs)
nov_green_2018 = format_date(nov_green_2018)
print(nov_green_2018)


## DEC 2018
dec_green_2018 = parquet_to_df('green_tripdata_2018-12.parquet')
dec_green_2018 = extract_data(dec_green_2018)
dec_green_2018_boroughs = add_borough(dec_green_2018)
dec_green_2018 = add_to_df(dec_green_2018, 'Borough', dec_green_2018_boroughs)
dec_green_2018 = format_date(dec_green_2018)
print(dec_green_2018)


green_2015 = combine_dfs(jan_green_2015,feb_green_2015,mar_green_2015, apr_green_2015, may_green_2015, jun_green_2015, jul_green_2015, aug_green_2015, sep_green_2015, oct_green_2015, nov_green_2015, dec_green_2015)
green_2016 = combine_dfs(jan_green_2016,feb_green_2016,mar_green_2016, apr_green_2016, may_green_2016, jun_green_2016, jul_green_2016, aug_green_2016, sep_green_2016, oct_green_2016, nov_green_2016, dec_green_2016)
green_2017 = combine_dfs(jan_green_2017,feb_green_2017,mar_green_2017, apr_green_2017, may_green_2017, jun_green_2017, jul_green_2017, aug_green_2017, sep_green_2017, oct_green_2017, nov_green_2017, dec_green_2017)
green_2018 = combine_dfs(jan_green_2018,feb_green_2018,mar_green_2018, apr_green_2018, may_green_2018, jun_green_2018, jul_green_2018, aug_green_2018, sep_green_2018, oct_green_2018, nov_green_2018, dec_green_2018)
green_15_16_17_18 = combine_dfs(green_2015,green_2016,green_2017,green_2018)
print(green_15_16_17_18)


# Export as csv file

green_15_16_17_18_csv = green_15_16_17_18.to_csv('green_taxi_15_18.csv',index = False)
