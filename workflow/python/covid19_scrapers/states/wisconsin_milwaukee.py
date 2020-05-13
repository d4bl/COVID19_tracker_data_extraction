from covid19_scrapers.utils import *
from covid19_scrapers.scraper import ScraperBase

import logging


_logger = logging.getLogger('covid19_scrapers')


class Wisconsin_Milwaukee(ScraperBase):
    DOWNLOAD_URL_TEMPLATE = ''
    
    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def name(self):
        return 'Wisconsin -- Milwaukee County'
    
    def _scrape(self, validation):
        # Get the timestamp
        cases_date = get_metadata_date('https://services5.arcgis.com/8Q02ELWlq5TYUASS/arcgis/rest/services/Cases_View/FeatureServer/0?f=json')
        deaths_date = get_metadata_date('https://services5.arcgis.com/8Q02ELWlq5TYUASS/arcgis/rest/services/Deaths_View1/FeatureServer/0?f=json')
        if cases_date != deaths_date:
            _logger.debug('Unexpected mismath between cases and deaths metadata dates:', cases_date, '!=', deaths_date)
        date_published = cases_date.strftime('%m/%d/%Y')
        
        cases_total = get_json('https://services5.arcgis.com/8Q02ELWlq5TYUASS/arcgis/rest/services/Cases_View/FeatureServer/0/query?f=json&where=1%3D1&returnGeometry=false&spatialRel=esriSpatialRelIntersects&outFields=*&outStatistics=%5B%7B%22statisticType%22%3A%22count%22%2C%22onStatisticField%22%3A%22ObjectId%22%2C%22outStatisticFieldName%22%3A%22value%22%7D%5D&resultType=standard&cacheHint=true')
        cnt_cases = cases_total['features'][0]['attributes']['value']
        deaths_total = get_json('https://services5.arcgis.com/8Q02ELWlq5TYUASS/arcgis/rest/services/Deaths_View1/FeatureServer/0/query?f=json&where=1%3D1&returnGeometry=false&spatialRel=esriSpatialRelIntersects&outFields=*&outStatistics=%5B%7B%22statisticType%22%3A%22count%22%2C%22onStatisticField%22%3A%22ObjectId%22%2C%22outStatisticFieldName%22%3A%22value%22%7D%5D&resultType=standard&cacheHint=true')
        cnt_deaths = deaths_total['features'][0]['attributes']['value']
        
        cases_by_race = get_json('https://services5.arcgis.com/8Q02ELWlq5TYUASS/arcgis/rest/services/Cases_View/FeatureServer/0/query?f=json&where=Race_Eth%20NOT%20LIKE%20%27%25%23N%2FA%27&returnGeometry=false&spatialRel=esriSpatialRelIntersects&outFields=*&groupByFieldsForStatistics=Race_Eth&orderByFields=value%20desc&outStatistics=%5B%7B%22statisticType%22%3A%22count%22%2C%22onStatisticField%22%3A%22ObjectId%22%2C%22outStatisticFieldName%22%3A%22value%22%7D%5D&resultType=standard&cacheHint=true')
        for feature in cases_by_race['features']:
            if feature['attributes']['Race_Eth'] == 'Black Alone':
                cnt_cases_aa = feature['attributes']['value']
                pct_cases_aa = round(100 * feature['attributes']['value'] / cnt_cases, 2)
                break
        
        deaths_by_race = get_json('https://services5.arcgis.com/8Q02ELWlq5TYUASS/arcgis/rest/services/Deaths_View1/FeatureServer/0/query?f=json&where=1%3D1&returnGeometry=false&spatialRel=esriSpatialRelIntersects&outFields=*&groupByFieldsForStatistics=Race_Eth&orderByFields=value%20desc&outStatistics=%5B%7B%22statisticType%22%3A%22count%22%2C%22onStatisticField%22%3A%22ObjectId%22%2C%22outStatisticFieldName%22%3A%22value%22%7D%5D&resultType=standard&cacheHint=true')
        for feature in deaths_by_race['features']:
            if feature['attributes']['Race_Eth'] == 'Black Alone':
                cnt_deaths_aa = feature['attributes']['value']
                pct_deaths_aa = round(100 * feature['attributes']['value'] / cnt_deaths, 2)
                break

        return [self._make_series(
            date=date_published,
            cases=cnt_cases,
            deaths=cnt_deaths,
            aa_cases=cnt_cases_aa,
            aa_deaths=cnt_deaths_aa,
            pct_aa_cases=pct_cases_aa,
            pct_aa_deaths=pct_deaths_aa,
        )]

    def _format_error(self, e):
        if isinstance(e, OverflowError):
            return f'ERROR: processing last update timstamp: {repr(e)}';
        elif isinstance(e, requests.RequestException):
            return f'ERROR: retrieving URL {e.request.url}: {repr(e)}';
        else:
            return super()._format_error(e)
