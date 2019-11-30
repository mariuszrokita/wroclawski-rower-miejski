import os
import pandas as pd

from bikeavailability.src.utils.logging import logger


def save_dataframe(df, path, filename):
    full_filepath = os.path.join(path, '{}.csv'.format(filename))
    df.to_csv(full_filepath, index=False)
    logger.info(f"    Data saved to: {full_filepath}")


def load_dataframe(path, filename, columns, parse_dates=False):
    full_filepath = os.path.join(path, '{}.csv'.format(filename))
    if os.path.exists(full_filepath):
        return pd.read_csv(full_filepath, parse_dates=parse_dates)
    else:
        return pd.DataFrame(data=[], columns=columns)
