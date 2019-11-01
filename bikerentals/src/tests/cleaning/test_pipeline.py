import pandas as pd
from pandas.util.testing import assert_frame_equal

from bikerentals.src.cleaning.pipeline import execute


class TestDataCleaningPipeline:

    def test_should_keep_all_records_when_soft_delete_selected(self):
        # arrange
        data = {
            'UID': [1, 2, 3, 4, 5],
            'Rental station': ['Station A', 'Station B', 'Station C', 'Station D', 'Station E'],
            'Return station': ['Station Z', 'Station B', 'Station D', 'Station D', 'Station B'],
            'Duration': [2, 2, 1, 4, 5]
        }

        expected = {
            'UID': [1, 2, 3, 4, 5],
            'Rental station': ['Station A', 'Station B', 'Station C', 'Station D', 'Station E'],
            'Return station': ['Station Z', 'Station B', 'Station D', 'Station D', 'Station B'],
            'Duration': [2, 2, 1, 4, 5],
            'IsDeleted': [False, True, False, True, False]
        }

        df = pd.DataFrame(data).set_index('UID')
        expected_df = pd.DataFrame(expected).set_index('UID')

        # act
        actual_df = execute(df, hard_delete=False)  # soft delete

        # assert
        assert_frame_equal(actual_df, expected_df)

    def test_should_remove_marked_records_when_hard_delete_selected(self):
        # arrange
        data = {
            'UID': [1, 2, 3, 4, 5],
            'Rental station': ['Station A', 'Station B', 'Station C', 'Station D', 'Station E'],
            'Return station': ['Station Z', 'Station B', 'Station D', 'Station D', 'Station B'],
            'Duration': [2, 2, 1, 4, 5]
        }

        expected = {
            'UID': [1, 3, 5],
            'Rental station': ['Station A', 'Station C', 'Station E'],
            'Return station': ['Station Z', 'Station D', 'Station B'],
            'Duration': [2, 1, 5]
        }

        df = pd.DataFrame(data).set_index('UID')
        expected_df = pd.DataFrame(expected).set_index('UID')

        # act
        actual_df = execute(df, hard_delete=True)

        # assert
        assert_frame_equal(actual_df, expected_df)
