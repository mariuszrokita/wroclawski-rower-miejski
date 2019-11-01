import pandas as pd
from sklearn.pipeline import make_pipeline

from bikerentals.src.cleaning.remove_same_location import SameLocationRemover


def execute(df: pd.DataFrame, hard_delete: bool) -> pd.DataFrame:
    """
    Execute data cleaning pipeline.

    Parameters:
    * df - dataframe
    * hard_delete - delete permanently records, otherwise soft delete will be applied
    """

    flag_col = 'IsDeleted'

    data_cleaning_pipeline = make_pipeline(
        SameLocationRemover('Rental station', 'Return station', flag_col)
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
