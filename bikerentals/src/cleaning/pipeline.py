from sklearn.pipeline import make_pipeline

from bikerentals.src.cleaning.remove_same_location import SameLocationRemover


def execute(df, remove_records):
    """
    Execute data cleaning pipeline.

    Parameters:
    * df - dataframe
    * remove_records - indicate whether to remove some records while cleaning
                       data or not.
    """

    if remove_records:
        data_cleaning_pipeline = make_pipeline(
            SameLocationRemover('Rental station', 'Return station')
        )
    else:
        # there's no work defined, just return
        return df

    # execute pipeline and return result
    processed_df = data_cleaning_pipeline.transform(df)
    return processed_df
