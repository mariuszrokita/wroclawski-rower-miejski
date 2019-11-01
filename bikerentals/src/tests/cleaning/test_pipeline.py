import numpy as np
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
            'Rental station latitude':  [np.nan,   1.11, 1.11,   1.11, 1.11],  # noqa E241
            'Rental station longitude': [  2.22, np.nan, 2.22,   2.22, 2.22],  # noqa E241
            'Return station latitude':  [  3.33, np.nan, 3.33, np.nan, 3.33],  # noqa E241
            'Return station longitude': [  4.44, np.nan, 4.44, np.nan, 4.44]   # noqa E241
        }

        expected = {
            'UID': [1, 2, 3, 4, 5],
            'Rental station': ['Station A', 'Station B', 'Station C', 'Station D', 'Station E'],
            'Return station': ['Station Z', 'Station B', 'Station D', 'Station D', 'Station B'],
            'Rental station latitude':  [np.nan,   1.11, 1.11,   1.11, 1.11],  # noqa E241
            'Rental station longitude': [  2.22, np.nan, 2.22,   2.22, 2.22],  # noqa E241
            'Return station latitude':  [  3.33, np.nan, 3.33, np.nan, 3.33],  # noqa E241
            'Return station longitude': [  4.44, np.nan, 4.44, np.nan, 4.44],  # noqa E241
            'IsDeleted': [True, True, False, True, False]
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
            'Rental station latitude':  [np.nan, 1.11, 1.10,   1.11, 1.11],  # noqa E241
            'Rental station longitude': [  2.22, 2.22, 2.20,   2.22, 2.22],  # noqa E241
            'Return station latitude':  [  3.33, 3.33, 3.30, np.nan, 3.33],  # noqa E241
            'Return station longitude': [  4.44, 4.44, 4.40, np.nan, 4.44]   # noqa E241
        }

        expected = {
            'UID': [3, 5],
            'Rental station': ['Station C', 'Station E'],
            'Return station': ['Station D', 'Station B'],
            'Rental station latitude':  [1.10, 1.11],  # noqa E241
            'Rental station longitude': [2.20, 2.22],
            'Return station latitude':  [3.30, 3.33],  # noqa E241
            'Return station longitude': [4.40, 4.44]
        }

        df = pd.DataFrame(data).set_index('UID')
        expected_df = pd.DataFrame(expected).set_index('UID')

        # act
        actual_df = execute(df, hard_delete=True)

        # assert
        assert_frame_equal(actual_df, expected_df)

    def test_should_extract_gps_from_rental_station_name(self):
        # arrange
        data = {
            'UID': [1],
            'Rental station': ['50.984208, 16.654945'],
            'Return station': ['Station A'],
            'Rental station latitude':  [np.nan],  # noqa E241
            'Rental station longitude': [np.nan],  # noqa E241
            'Return station latitude':  [11.111],  # noqa E241
            'Return station longitude': [22.22]    # noqa E241
        }

        expected = {
            'UID': [1],
            'Rental station': ['50.984208, 16.654945'],
            'Return station': ['Station A'],
            'Rental station latitude':  [50.984208],  # noqa E241
            'Rental station longitude': [16.654945],  # noqa E241
            'Return station latitude':  [11.111],     # noqa E241
            'Return station longitude': [22.22]       # noqa E241
        }

        df = pd.DataFrame(data).set_index('UID')
        expected_df = pd.DataFrame(expected).set_index('UID')

        # act
        actual_df = execute(df, hard_delete=True)

        # assert
        assert_frame_equal(actual_df, expected_df)

    def test_should_extract_gps_from_return_station_name(self):
        # arrange
        data = {
            'UID': [1],
            'Rental station': ['Station A'],
            'Return station': ['50.984208, 16.654945'],
            'Rental station latitude':  [11.111],  # noqa E241
            'Rental station longitude': [22.22],   # noqa E241
            'Return station latitude':  [np.nan],  # noqa E241
            'Return station longitude': [np.nan]   # noqa E241
        }

        expected = {
            'UID': [1],
            'Rental station': ['Station A'],
            'Return station': ['50.984208, 16.654945'],
            'Rental station latitude':  [11.111],     # noqa E241
            'Rental station longitude': [22.22],      # noqa E241
            'Return station latitude':  [50.984208],  # noqa E241
            'Return station longitude': [16.654945]   # noqa E241
        }

        df = pd.DataFrame(data).set_index('UID')
        expected_df = pd.DataFrame(expected).set_index('UID')

        # act
        actual_df = execute(df, hard_delete=True)

        # assert
        assert_frame_equal(actual_df, expected_df)
