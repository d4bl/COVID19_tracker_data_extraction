from datetime import date
import mock

from covid19_scrapers.states.alabama import Alabama
from covid19_scrapers.test.states import util
from covid19_scrapers.utils.misc import to_percentage


@mock.patch('covid19_scrapers.states.alabama.query_geoservice')
def test_alabama(patched_geoservice):
    # setup constants and mock data
    AA_IDX = 1
    UNKNOWN_IDX = 3

    cases = {
        'Racecat': ['Asian', 'Black', 'Other', 'Unknown', 'White'],
        'value': [200, 19000, 3000, 18000, 20000]
    }

    deaths = {
        'Racecat': ['Asian', 'Black', 'Other', 'Unknown', 'White'],
        'value': [10, 500, 25, 50, 600]
    }

    known_cases_by_race = sum(cases['value']) - cases['value'][UNKNOWN_IDX]
    known_deaths_by_race = sum(deaths['value']) - deaths['value'][UNKNOWN_IDX]

    # patch geoservice
    patched_geoservice.side_effect = [
        util.make_query_geoservice_data(data=cases),
        util.make_query_geoservice_data(data=deaths)]

    # run and test
    util.run_scraper_and_assert(
        scraper_cls=Alabama,
        assertions={
            'Date Published': date.today(),
            'Total Cases': sum(cases['value']),
            'Total Deaths': sum(deaths['value']),
            'Count Cases Black/AA': cases['value'][AA_IDX],
            'Count Deaths Black/AA': deaths['value'][AA_IDX],
            'Pct Includes Unknown Race': False,
            'Pct Includes Hispanic Black': False,
            'Pct Cases Black/AA': to_percentage(cases['value'][AA_IDX], known_cases_by_race),
            'Pct Deaths Black/AA': to_percentage(deaths['value'][AA_IDX], known_deaths_by_race),
            'Count Cases Known Race': known_cases_by_race,
            'Count Deaths Known Race': known_deaths_by_race
        })
