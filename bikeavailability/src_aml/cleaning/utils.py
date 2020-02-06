def detect_periods_with_missing_data(df):
    #    # algorithm 1
    #     # Set time period/window and calculate number of all available bikes
    #     rule = f"{time_window_hours}H"
    #     resampled_df = df.resample(rule).sum().sum(axis=1)
    #     # Calculate slope/steepness of the graph of a function
    #     # (sum of all available bikes for each time window)
    #     slope = pd.Series(np.gradient(resampled_df.values),
    #                       resampled_df.index,
    #                       name='slope')
    #     # Determine time windows on which the number of available
    #     # bikes did not change (so the slope is 0).
    #     return slope[ slope == 0 ].index

    #    # algorithm 2
    #     # Set time period/window and calculate number of all available bikes
    #     rule = f"{time_window_hours}H"
    #     diff = df.resample(rule).sum().sum(axis=1) - df.resample(rule).sum().sum(axis=1).shift(1)
    #     diff = df.resample(rule).sum().sum(axis=1) - df.resample(rule).sum().sum(axis=1).shift(1)
    #     return diff[diff.values == 0].index

    # algorithm 3
    # TODO: This is the ugly way, but currently it's not my goal to algorithmically
    # detect periods with missing data in a clever way. I'll get back to this place once
    # the entire AML pipeline is ready.
    # Rule "Done is better than perfect" is applied here.
    return [
        ('2019-11-13 08:40:00', '2019-11-23 11:30:00'),
        ('2019-11-30 19:00:00', '2019-12-01 12:00:00'),
        ('2019-12-18 12:00:00', '2019-12-22 13:10:00'),
        ('2019-12-31 04:10:00', '2020-01-03 11:30:00'),
        ('2019-11-06 22:00:00', '2019-11-06 22:10:00'),
        ('2019-11-07 06:00:00', '2019-11-07 06:00:00'),
        ('2020-01-10 22:30:00', '2020-01-10 22:30:00'),
        ('2020-01-30 13:00:00', '2020-01-30 13:00:00')
    ]
