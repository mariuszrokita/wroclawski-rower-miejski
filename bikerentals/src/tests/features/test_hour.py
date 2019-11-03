import pandas as pd
import pytest
from pandas.util.testing import assert_frame_equal

from bikerentals.src.features.hour import HourFeature


class TestHour:

    def test_should_determine_hour_based_on_rental_datetime(self):
        # arrange
        data = {
            'Rental datetime': ['2019-05-01 09:17:42', '2019-02-01 17:41:02']
        }

        expected = {
            'Rental datetime': ['2019-05-01 09:17:42', '2019-02-01 17:41:02'],
            'Rental hour': [9, 17]
        }

        df = pd.DataFrame(data)
        df['Rental datetime'] = pd.to_datetime(df['Rental datetime'])

        expected_df = pd.DataFrame(expected)
        expected_df['Rental datetime'] = pd.to_datetime(expected_df['Rental datetime'])

        sut = HourFeature('Rental datetime', 'Rental hour')

        # act
        actual_df = sut.transform(df)

        # assert
        assert_frame_equal(actual_df, expected_df)

    def test_should_throw_when_missing_input_column(self):
        # arrange
        data = {
            'col_name': []
        }

        df = pd.DataFrame(data)
        df['col_name'] = pd.to_datetime(df['col_name'])

        # act
        sut = HourFeature('Rental datetime', 'Rental hour')

        with pytest.raises(KeyError, match='Rental datetime'):
            sut.transform(df)
