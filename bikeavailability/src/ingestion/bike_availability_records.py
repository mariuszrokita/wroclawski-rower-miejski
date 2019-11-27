import json
import glob
import os
import pandas as pd


class BikeAvailabilityRecords:

    def __init__(self, source_folder):
        self.source_folder = source_folder

    def load_data(self) -> (pd.DataFrame, [str]):
        raw_data_df, processed_filenames = self.__load_files()
        clean_data_df = self.__clean(raw_data_df)
        clean_data_df = self.__change_datatypes(clean_data_df)
        return clean_data_df, processed_filenames

    def __load_files(self):
        # list of files already read and processed
        processed_filenames = []
        big_frame = pd.DataFrame(data=[], columns=['bikes', 'number', 'timestamp'])

        for filename in sorted(glob.glob(os.path.join(self.source_folder, '*.json'))):
            # load json file
            with open(filename) as json_file:
                data = json.load(json_file)

            if data['success'] is True:
                # make dataframe from a single json
                records_df = pd.DataFrame(data=data['result']['records'])
                records_df['timestamp'] = data['datetime']
                big_frame = pd.concat([big_frame, records_df], sort=True, ignore_index=True)
                processed_filenames.append(os.path.basename(filename))

        return big_frame, processed_filenames

    def __clean(self, df):
        # TODO: remove fraction of seconds from timestamp
        return df

    def __change_datatypes(self, df):
        df = df.set_index("timestamp")
        return df
