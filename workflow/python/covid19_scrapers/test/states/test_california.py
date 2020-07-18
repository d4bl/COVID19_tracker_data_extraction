from datetime import datetime

import mock

from covid19_scrapers.states.california import California
from covid19_scrapers.test.states import util
from covid19_scrapers.utils.misc import to_percentage


def test_california():
    mock_dfs = [
        util.mock_read_csv_dataframe(
            'california_cases.csv',
            parse_dates=True),

        util.mock_read_csv_dataframe(
            'california_demographics.csv',
            index_col=['date', 'race_ethnicity'],
            parse_dates=True)
    ]

    with mock.patch('covid19_scrapers.states.california.pd.read_csv', side_effect=mock_dfs):
        util.run_scraper_and_assert(
            scraper_cls=California,
            assertions={
                'Date Published': datetime(2020, 7, 16).date(),
                'Total Cases': 10000,
                'Total Deaths': 1500,
                'Count Cases Black/AA': 10000,
                'Count Deaths Black/AA': 600,
                'Pct Includes Unknown Race': False,
                'Pct Includes Hispanic Black': False,
                'Pct Cases Black/AA': to_percentage(10000, 45000),
                'Pct Deaths Black/AA': to_percentage(600, 4100),
                'Count Cases Known Race': 45000,
                'Count Deaths Known Race': 4100
            })
