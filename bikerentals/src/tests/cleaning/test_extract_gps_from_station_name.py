import numpy as np
import pandas as pd
from pandas.util.testing import assert_frame_equal

from bikerentals.src.cleaning.extract_gps_from_station_name import GpsFromStationNameExtractor


class TestGpsFromStationNameExtractor:

    def test_should_extract_gps_when_rental_station_name_contains_coordinates(self):
        # arrange
        data = {
            'UID': [1],
            'Rental station': ['51.136671111111, 17.05744'],
            'Rental station latitude': [np.nan],
            'Rental station longitude': [np.nan]
        }

        expected = {
            'UID': [1],
            'Rental station': ['Poza oficjalną stacją'],
            'Rental station latitude': [51.136671111111],
            'Rental station longitude': [17.05744]
        }

        df = pd.DataFrame(data).set_index('UID')
        expected_df = pd.DataFrame(expected).set_index('UID')

        sut = GpsFromStationNameExtractor('Rental station', 'Rental station latitude', 'Rental station longitude')

        # act
        actual_df = sut.transform(df)

        # assert
        assert_frame_equal(actual_df, expected_df)

    def test_should_extract_gps_when_return_station_name_contains_coordinates(self):
        # arrange
        data = {
            'UID': [1],
            'Return station': ['51.136671111111, 17.05744'],
            'Return station latitude': [np.nan],
            'Return station longitude': [np.nan]
        }

        expected = {
            'UID': [1],
            'Return station': ['Poza oficjalną stacją'],
            'Return station latitude': [51.136671111111],
            'Return station longitude': [17.05744]
        }

        df = pd.DataFrame(data).set_index('UID')
        expected_df = pd.DataFrame(expected).set_index('UID')

        sut = GpsFromStationNameExtractor('Return station', 'Return station latitude', 'Return station longitude')

        # act
        actual_df = sut.transform(df)

        # assert
        assert_frame_equal(actual_df, expected_df)
