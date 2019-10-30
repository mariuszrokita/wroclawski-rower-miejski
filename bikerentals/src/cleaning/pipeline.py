from sklearn.pipeline import make_pipeline

from bikerentals.src.cleaning.remove_same_location import SameLocationRemover

def execute(df):
    
    data_cleaning_pipeline = make_pipeline(
        SameLocationRemover('Rental station', 'Return station')
    )

    # execute pipeline and return result
    processed_df = data_cleaning_pipeline.transform(df)
    return processed_df
