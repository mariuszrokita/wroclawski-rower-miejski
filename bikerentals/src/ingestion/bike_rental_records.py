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

        # Remove whitespace characters
        df.replace(u'\xa0$', u'', regex=True, inplace=True)

        # Remove leading and trailing whitespace characters
        df['Rental station'] = df['Rental station'].str.strip()
        df['Return station'] = df['Return station'].str.strip()

        # Fix wordings
        for col in ['Rental station', 'Return station']:
            df[col] = df[col].str.replace('dworzec główny', 'Dworzec Główny')
            df[col] = df[col].str.replace('Dworzec Główny, północ', 'Dworzec Główny')
            df[col] = df[col].str.replace('Drobnera - Dubois', 'Drobnera / Dubois')
            df[col] = df[col].str.replace('Świdnicka - Piłsudskiego', 'Świdnicka / Piłsudskiego (Hotel Scandic)')

        return df

    def __change_datatypes(self, df):
        df = df.set_index("UID")

        df['Rental datetime'] = pd.to_datetime(df['Rental datetime'])
        df['Return datetime'] = pd.to_datetime(df['Return datetime'])

        df['Duration'] = pd.to_timedelta(df['Duration'])
        return df
