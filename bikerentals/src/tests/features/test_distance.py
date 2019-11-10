import pandas as pd
from pandas.util.testing import assert_frame_equal

from bikerentals.src.features.distance import DistanceFeature


class TestDistance:

    def test_should_return_0_for_same_rental_and_return_station_coordinates(self):
        # arrange
        data = {
            'Rental station latitude':  [-1.11, 2.22],  # noqa E241
            'Rental station longitude': [-1.11, 2.22],
            'Return station latitude':  [-1.11, 2.22],  # noqa E241
            'Return station longitude': [-1.11, 2.22]
        }

        expected = {
            'Rental station latitude':  [-1.11, 2.22],  # noqa E241
            'Rental station longitude': [-1.11, 2.22],
            'Return station latitude':  [-1.11, 2.22],  # noqa E241
            'Return station longitude': [-1.11, 2.22],
            'Distance': [0.0, 0.0]
        }

        df = pd.DataFrame(data)
        expected_df = pd.DataFrame(expected)

        sut = DistanceFeature('Rental station latitude', 'Rental station longitude',
                              'Return station latitude', 'Return station longitude', 'Distance')

        # act
        actual_df = sut.transform(df)

        # assert
        assert_frame_equal(actual_df, expected_df)

    def test_should_calculate_distance(self):
        # arrange
        data = {
            'Rental station latitude':  [51.116822],  # noqa E241
            'Rental station longitude': [17.051845],
            'Return station latitude':  [51.112911],  # noqa E241
            'Return station longitude': [17.001360]
        }

        expected = {
            'Rental station latitude':  [51.116822],  # noqa E241
            'Rental station longitude': [17.051845],
            'Return station latitude':  [51.112911],  # noqa E241
            'Return station longitude': [17.001360],
            'Distance': [3.6]
        }

        df = pd.DataFrame(data)
        expected_df = pd.DataFrame(expected)

        sut = DistanceFeature('Rental station latitude', 'Rental station longitude',
                              'Return station latitude', 'Return station longitude', 'Distance')

        # act
        actual_df = sut.transform(df)

        # assert
        assert_frame_equal(actual_df, expected_df)
