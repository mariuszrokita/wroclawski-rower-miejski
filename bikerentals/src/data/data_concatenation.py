import pandas as pd


def combine_datasets(bike_rentals_df, bike_stations_df):
    # part 1 - merge to get gps coordinates for RENTAL stations
    bike_rentals_df = pd.merge(bike_rentals_df, 
                               bike_stations_df, 
                               how='left',
                               left_on='Rental station',
                               right_on='Bike station')

    # define more meaningful names for coordinates columns
    cols = [f'Rental station {str.lower(column)}' 
                if column in ['Latitude', 'Longitude'] else column
                    for column in bike_rentals_df.columns]
    bike_rentals_df.columns = cols

    # drop unnecessary columns
    bike_rentals_df.drop(['Bike station', 'Station no'], axis=1, inplace=True)

    # part 2 - merge to get gps coordinates for RETURN station
    bike_rentals_df = pd.merge(bike_rentals_df, 
                            bike_stations_df, 
                            how='left',
                            left_on='Return station',
                            right_on='Bike station')

    # define more meaningful names for coordinates columns
    cols = [f'Return station {str.lower(column)}' 
                if column in ['Latitude', 'Longitude'] else column
                    for column in bike_rentals_df.columns]
    bike_rentals_df.columns = cols

    # drop unnecessary columns
    bike_rentals_df.drop(['Bike station', 'Station no'], axis=1, inplace=True)

    return bike_rentals_df
