import os
import pandas as pd


def save_dataframe(df, path, filename):
    full_filepath = os.path.join(path, '{}.csv'.format(filename))
    df.to_csv(full_filepath, index=False)
    print(f"Data saved to: {full_filepath}")


def save_dataframe_as_pipeline_data(df, path):
    df.to_csv(path, index=False)
    print(f"Data saved to: {path}")


def load_dataframe(path, filename, columns, parse_dates=False):
    full_filepath = os.path.join(path, '{}.csv'.format(filename))
    if os.path.exists(full_filepath):
        return pd.read_csv(full_filepath, parse_dates=parse_dates)
    else:
        return pd.DataFrame(data=[], columns=columns)
