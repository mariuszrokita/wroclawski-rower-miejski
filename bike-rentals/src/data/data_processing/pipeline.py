from sklearn.pipeline import make_pipeline

from data_processing.feature_extraction.season import SeasonExtractor


def execute(df):

    data_processing_pipeline = make_pipeline(
        SeasonExtractor('Rental datetime', 'Season')
    )

    # execute pipeline and return result
    processed_df = data_processing_pipeline.transform(df)
    return processed_df