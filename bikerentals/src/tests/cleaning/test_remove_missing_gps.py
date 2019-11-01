import numpy as np
import pandas as pd
import pytest
from pandas.util.testing import assert_frame_equal

from bikerentals.src.cleaning.remove_missing_gps import MissingGpsLocationRemover


class TestMissingGpsLocationRemover:

    def test_should_mark_records_for_deletion_when_any_gps_location_is_missing(self):
        # arrange
        data = {
            'UID': [1, 2, 3, 4],
            'Rental station latitude':  [np.nan,   1.11, 2.22,   3.33],  # noqa E241
            'Rental station longitude': [  4.44, np.nan, 5.55,   6.66],  # noqa E241
            'Return station latitude':  [  7.77, np.nan, 8.88, np.nan],  # noqa E241
            'Return station longitude': [  9.99, np.nan, 1.10, np.nan]   # noqa E241
        }

        expected = {
            'UID': [1, 2, 3, 4],
            'Rental station latitude':  [np.nan,   1.11, 2.22,   3.33],  # noqa E241
            'Rental station longitude': [  4.44, np.nan, 5.55,   6.66],  # noqa E241
            'Return station latitude':  [  7.77, np.nan, 8.88, np.nan],  # noqa E241
            'Return station longitude': [  9.99, np.nan, 1.10, np.nan],  # noqa E241
            'IsDeleted': [True, True, False, True]
        }

        location_cols = ['Rental station latitude', 'Rental station longitude',
                         'Return station latitude', 'Return station longitude']

        df = pd.DataFrame(data).set_index('UID')
        expected_df = pd.DataFrame(expected).set_index('UID')

        # act
        sut = MissingGpsLocationRemover(location_cols, flag_col='IsDeleted')
        actual_df = sut.transform(df)

        # assert
        assert_frame_equal(actual_df, expected_df)

    def test_should_throw_when_missing_any_location_column(self):
        # arrange
        data = {
            'UID': [],
            'Rental station latitude': [],
            'Rental station longitude': [],
            'Return station latitude': [],
            'Return station longitude': []
        }

        location_cols = ['Rental station latitude', 'some col name',
                         'Return station latitude', 'Return station longitude']

        df = pd.DataFrame(data)

        # act
        sut = MissingGpsLocationRemover(location_cols, flag_col='IsDeleted')

        with pytest.raises(KeyError, match='some col name'):
            sut.transform(df)
