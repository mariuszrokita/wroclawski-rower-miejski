import pandas as pd
import requests

from bs4 import BeautifulSoup


class BikeStationsLocations:

    URL = 'https://wroclawskirower.pl/en/stations-map/'

    def load_data(self):
        """Downloads and returns bike stations details: street name , gps coordinates.
        
        Returns:
        * pandas.DataFrame - returning value
        """
        raw_data_df = self.__download_bike_stations_data()
        clean_data_df = self.__clean(raw_data_df)
        return clean_data_df

    def __download_bike_stations_data(self):

        page = requests.get(self.URL)
        soup = BeautifulSoup(page.text, features="html.parser")

        data = []
        table = soup.find(name='div', class_='station_list').find(name='table')
        rows = table.find_all(name='tr')

        # header
        header_row = rows[0]
        header_cols = [ele.text.strip() for ele in header_row.find_all('th')]

        # data rows
        for row in rows[1:]:  
            cols = row.find_all('td')
            cols = [ele.text.strip() for ele in cols]
            data.append(cols)

        return pd.DataFrame(data=data, columns=header_cols)

    def __clean(self, df):
        # Make sure input(raw) data has known structure
        self.__assert_input_data_structure(df)

        # Remove records that have no station number - records related to bikes 
        # that were returned to some places outside official bike stations.
        df = df.dropna(subset=['Station no'])

        # Get only bike station name and coordinates. Make column names
        # clear for English-speaking audience.
        df = df[['Nazwa stacji', 'Coordinates']]
        df.columns = ['Bike station', 'Coordinates']

        # Make separate columns for latitude and longitude.
        gps_coordinates = df['Coordinates'].str.split(', ', n = 1, expand = True)
        df.loc[:, 'Latitude'] = gps_coordinates[0]
        df.loc[:, 'Longitude'] = gps_coordinates[1]
        df = df.drop("Coordinates", axis=1)

        return df

    def __assert_input_data_structure(self, df):
        # TODO: write logic that validates input data
        return True
