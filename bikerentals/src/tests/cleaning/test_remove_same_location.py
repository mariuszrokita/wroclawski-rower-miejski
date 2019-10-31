import pandas as pd
import pytest
from pandas.util.testing import assert_frame_equal

from bikerentals.src.cleaning.remove_same_location import SameLocationRemover


class TestSameLocationRemover:

    def test_should_remove_records_when_rental_station_and_return_station_is_same(self):
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
        sut = SameLocationRemover('Rental station', 'Return station')
        actual_df = sut.transform(df)

        # assert
        assert_frame_equal(actual_df, expected_df)

    def test_should_throw_when_missing_rental_station_column(self):
        # arrange
        data = {
            'Rental station': [],
            'Return station': []
        }

        df = pd.DataFrame(data)

        # act
        sut = SameLocationRemover('some col name', 'Return station')

        with pytest.raises(KeyError, match='some col name'):
            sut.transform(df)

    def test_should_throw_when_missing_return_station_column(self):
        # arrange
        data = {
            'Rental station': [],
            'Return station': []
        }

        df = pd.DataFrame(data)

        # act
        sut = SameLocationRemover('Rental station', 'some col name')

        with pytest.raises(KeyError, match='some col name'):
            sut.transform(df)
