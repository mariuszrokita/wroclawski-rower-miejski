import pandas as pd
from pandas.util.testing import assert_frame_equal

from bikerentals.src.features.rental_fee import RentalFeeFeature


class TestRentalFee:

    def test_should_return_0_when_rental_time_below_20_minutes(self):
        # arrange
        data = {
            'Rental datetime': ['2019-05-01 01:15:31', '2019-05-02 01:15:31'],
            'Return datetime': ['2019-05-01 01:34:00', '2019-05-02 01:35:31']
        }

        expected = {
            'Rental datetime': ['2019-05-01 01:15:31', '2019-05-02 01:15:31'],
            'Return datetime': ['2019-05-01 01:34:00', '2019-05-02 01:35:31'],
            'Rental fee': [0, 0]
        }

        df = pd.DataFrame(data)
        df['Rental datetime'] = pd.to_datetime(df['Rental datetime'])
        df['Return datetime'] = pd.to_datetime(df['Return datetime'])

        expected_df = pd.DataFrame(expected)
        expected_df['Rental datetime'] = pd.to_datetime(expected_df['Rental datetime'])
        expected_df['Return datetime'] = pd.to_datetime(expected_df['Return datetime'])

        sut = RentalFeeFeature('Rental datetime', 'Return datetime', 'Rental fee')

        # act
        actual_df = sut.transform(df)

        # assert
        assert_frame_equal(actual_df, expected_df)

    def test_should_return_2_when_rental_time_between_21_and_60_minutes(self):
        # arrange
        data = {
            'Rental datetime': ['2019-05-01 01:15:31', '2019-05-02 01:15:31'],
            'Return datetime': ['2019-05-01 01:35:32', '2019-05-02 02:15:31']
        }

        expected = {
            'Rental datetime': ['2019-05-01 01:15:31', '2019-05-02 01:15:31'],
            'Return datetime': ['2019-05-01 01:35:32', '2019-05-02 02:15:31'],
            'Rental fee': [2, 2]
        }

        df = pd.DataFrame(data)
        df['Rental datetime'] = pd.to_datetime(df['Rental datetime'])
        df['Return datetime'] = pd.to_datetime(df['Return datetime'])

        expected_df = pd.DataFrame(expected)
        expected_df['Rental datetime'] = pd.to_datetime(expected_df['Rental datetime'])
        expected_df['Return datetime'] = pd.to_datetime(expected_df['Return datetime'])

        sut = RentalFeeFeature('Rental datetime', 'Return datetime', 'Rental fee')

        # act
        actual_df = sut.transform(df)

        # assert
        assert_frame_equal(actual_df, expected_df)

    def test_should_return_2_when_rental_time_between_61_minutes_and_12_hours(self):
        # arrange
        data = {
            'Rental datetime': ['2019-05-01 01:15:31', '2019-05-02 01:15:31', '2019-05-02 01:15:31'],
            'Return datetime': ['2019-05-01 02:15:32', '2019-05-02 04:15:31', '2019-05-02 13:15:31']
        }

        expected = {
            'Rental datetime': ['2019-05-01 01:15:31', '2019-05-02 01:15:31', '2019-05-02 01:15:31'],
            'Return datetime': ['2019-05-01 02:15:32', '2019-05-02 04:15:31', '2019-05-02 13:15:31'],
            'Rental fee': [6.0, 10.0, 46.0]
        }

        df = pd.DataFrame(data)
        df['Rental datetime'] = pd.to_datetime(df['Rental datetime'])
        df['Return datetime'] = pd.to_datetime(df['Return datetime'])

        expected_df = pd.DataFrame(expected)
        expected_df['Rental datetime'] = pd.to_datetime(expected_df['Rental datetime'])
        expected_df['Return datetime'] = pd.to_datetime(expected_df['Return datetime'])

        sut = RentalFeeFeature('Rental datetime', 'Return datetime', 'Rental fee')

        # act
        actual_df = sut.transform(df)

        # assert
        assert_frame_equal(actual_df, expected_df)

    def test_should_return_2_when_rental_time_over_12_hours(self):
        # arrange
        data = {
            'Rental datetime': ['2019-05-01 01:15:31', '2019-05-02 01:15:31'],
            'Return datetime': ['2019-05-01 13:15:32', '2019-05-03 01:15:30']
        }

        expected = {
            'Rental datetime': ['2019-05-01 01:15:31', '2019-05-02 01:15:31'],
            'Return datetime': ['2019-05-01 13:15:32', '2019-05-03 01:15:30'],
            'Rental fee': [350.0, 394.0]
        }

        df = pd.DataFrame(data)
        df['Rental datetime'] = pd.to_datetime(df['Rental datetime'])
        df['Return datetime'] = pd.to_datetime(df['Return datetime'])

        expected_df = pd.DataFrame(expected)
        expected_df['Rental datetime'] = pd.to_datetime(expected_df['Rental datetime'])
        expected_df['Return datetime'] = pd.to_datetime(expected_df['Return datetime'])

        sut = RentalFeeFeature('Rental datetime', 'Return datetime', 'Rental fee')

        # act
        actual_df = sut.transform(df)

        # assert
        assert_frame_equal(actual_df, expected_df)
