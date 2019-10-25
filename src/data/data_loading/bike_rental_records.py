import glob
import os
import pandas as pd

class BikeRentalRecords:

    def __init__(self, source_folder):
        self.source_folder = source_folder

    def load_data(self):
        raw_data_df = self.__load_csv_files()
        clean_data_df = self.__clean(raw_data_df)
        clean_data_df = self.__change_datatypes(clean_data_df)
        return clean_data_df

    def __load_csv_files(self):
        dfs = []

        # Get filenames and load data to 
        for filename in glob.glob(os.path.join(self.source_folder, 'Historia_przejazdow_*.csv')):
            dfs.append(pd.read_csv(filename, parse_dates=['Data wynajmu', 'Data zwrotu']))

        # Concatenate all data into one DataFrame
        big_frame = pd.concat(dfs, ignore_index=True)
        return big_frame

    def __clean(self, df):
        # Last record is considered as unique and rest of the same values as duplicate
        df = df.drop_duplicates(subset="UID wynajmu", keep='last')
        # Sort it
        df = df.sort_values(by='UID wynajmu')

        # Make it convenient for the English-speaking audience
        df.columns = ['UID', 'Bike number', 'Rental datetime', 'Return datetime', 
                     'Rental station', 'Return station', 'Duration']

        return df

    def __change_datatypes(self, df):
        df = df.set_index("UID")

        df['Rental datetime'] = pd.to_datetime(df['Rental datetime'])
        df['Return datetime'] = pd.to_datetime(df['Return datetime'])

        df['Duration'] = pd.to_timedelta(df['Duration'])
        return df