from covid19_scrapers.utils import (get_content, get_json)
from covid19_scrapers.scraper import ScraperBase

import datetime
import logging
import re


_logger = logging.getLogger(__name__)


class NewMexico(ScraperBase):
    BETA_SCRAPER = True
    UTILS_URL = 'https://cvprovider.nmhealth.org/js/utils.js'
    DATA_URL_TEMPLATE = '{service}/GetPublicStatewideData'

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def name(self):
        return 'New Mexico'

    def _scrape(self, **kwargs):
        content = get_content(self.UTILS_URL).decode('utf-8')
        match = re.search(r'serviceUrl(.|\n)*?"prod": "(.*?)"', content, re.M)
        if not match:
            raise ValueError(f'Unable to find service URL in {self.UTILS_URL}')
        data_url = self.DATA_URL_TEMPLATE.format(service=match.group(2))
        data = get_json(data_url)
        # sample data: {
        #   "status": "ok",
        #   "message": "",
        #   "data": {
        #     "cvDataId": 154,
        #     "created": 1592520572106,
        #     "updated": 1592520572106,
        #     "archived": null,
        #     "cases": 10153,
        #     "tests": 275897,
        #     "totalHospitalizations": 1726,
        #     "currentHospitalizations": 157,
        #     "deaths": 456,
        #     "recovered": 4439,
        #     "male": 5045,
        #     "female": 4979,
        #     "genderNR": 97,
        #     "0-9": 444,
        #     "10-19": 882,
        #     "20-29": 1672,
        #     "30-39": 1835,
        #     "40-49": 1577,
        #     "50-59": 1482,
        #     "60-69": 1145,
        #     "70-79": 546,
        #     "80-89": 371,
        #     "90+": 167,
        #     "ageNR": 0,
        #     "amInd": 5419,
        #     "asian": 65,
        #     "black": 203,
        #     "hawaiian": 18,
        #     "unknown": 454,
        #     "other": 55,
        #     "white": 1157,
        #     "hispanic": 2750
        #   }
        # }
        if data['status'] != 'ok':
            raise ValueError(
                f'/GetPublicStatewideData failed with status: {data["status"]}')
        date = datetime.date.fromtimestamp(data['data']['updated'] / 1000)
        # Get totals data
        total_cases = data['data']['cases']
        total_deaths = data['data']['deaths']

        # Get AA case data
        aa_cases = data['data']['black']
        aa_cases_pct = round(100 * aa_cases / total_cases, 2)

        # No AA death data
        # TODO: find AA death data
        aa_deaths_pct = float('nan')
        aa_deaths = float('nan')

        return [self._make_series(
            date=date,
            cases=total_cases,
            deaths=total_deaths,
            aa_cases=aa_cases,
            aa_deaths=aa_deaths,
            pct_aa_cases=aa_cases_pct,
            pct_aa_deaths=aa_deaths_pct,
            pct_includes_unknown_race=True,
            pct_includes_hispanic_black=False,
        )]
