import pandas as pd
from sklearn.pipeline import make_pipeline

from bikerentals.src.cleaning.extract_gps_from_station_name import GpsFromStationNameExtractor
from bikerentals.src.cleaning.remove_missing_gps import MissingGpsLocationRemover
from bikerentals.src.cleaning.remove_same_location import SameLocationRemover


def execute(df: pd.DataFrame, hard_delete: bool) -> pd.DataFrame:
    """
    Execute data cleaning pipeline.

    Parameters:
    * df - dataframe
    * hard_delete - delete permanently records, otherwise soft delete will be applied
    """

    gps_location_cols = ['Rental station latitude', 'Rental station longitude',
                         'Return station latitude', 'Return station longitude']
    flag_col = 'IsDeleted'

    data_cleaning_pipeline = make_pipeline(
        GpsFromStationNameExtractor('Rental station', 'Rental station latitude', 'Rental station longitude'),
        GpsFromStationNameExtractor('Return station', 'Return station latitude', 'Return station longitude'),
        SameLocationRemover('Rental station', 'Return station', flag_col),
        MissingGpsLocationRemover(gps_location_cols, flag_col)
    )

    # execute pipeline
    processed_df = data_cleaning_pipeline.transform(df)

    # delete permanently marked records if hard delete was chosen
    if hard_delete:
        # remove marked rows
        idx = processed_df[processed_df[flag_col] == True].index  # noqa: E731
        processed_df = processed_df.drop(index=idx, axis=0)
        # remove flag column
        processed_df = processed_df.drop([flag_col], axis=1)

    return processed_df
