#!/usr/bin/env python
"""Driver script for running all registered data scrapers and writing
the output to an Excel file.
"""

import argparse
import datetime
import logging
import pandas as pd
from covid19_scrapers import MakeScraperRegistry


_SCRAPER_REGISTRY = MakeScraperRegistry()
_KNOWN_SCRAPERS = set(_SCRAPER_REGISTRY.scraper_names())


def scraper(scraper_name):
    """Returns scraper is scraper is registered."""
    if scraper_name not in _KNOWN_SCRAPERS:
        raise ValueError("Invalid scraper:", scraper_name)
    return scraper_name

def output_file(filename):
    """Returns filename if we know how to write to it."""
    if (not filename.endswith('.xlsx')
        and not filename.endswith('.csv')
        and filename != '-'
    ):
        raise ValueError('Invalid output files: ' + filename)
    return filename

def parse_args():
    # Process command-line arguments
    parser = argparse.ArgumentParser(description="Run some or all scrapers")
    parser.add_argument('scrapers', metavar='SCRAPER', type=scraper, nargs='*',
                        help='List of scrapers to run, or all if omitted')
    parser.add_argument('--output', dest='outputs', metavar='FILE',
                        action='append', type=output_file,
                        help='Write output to FILE (must be -, or have csv or xlsx extension)')
    parser.add_argument('--log_level', type=str, metavar='LEVEL',
                        action='store', default='INFO',
                        choices=['CRITICAL', 'ERROR', 'WARNING', 'INFO',
                                 'DEBUG'],
                        help='Set log level to LEVEL')
    return parser.parse_args()
    
def write_output(df, output):
        logging.info(f'Writing {output}')
        if output == '-':
            # Set pandas options for stdout 
            pd.set_option('display.max_rows', None)
            pd.set_option('display.max_columns', None)
            pd.set_option('display.width', None)
            pd.set_option('display.max_colwidth', None)
            print(df)
        elif output.endswith('.csv'):
            df.to_csv(output)
        elif output.endswith('.xlsx'):
            df.to_excel(output)

def main():
    # Process options
    opts = parse_args()
    
    # Set up logging
    logging.basicConfig(
        format='%(asctime)s %(message)s',
        level=getattr(logging, opts.log_level.upper()))

    # Run scrapers
    if not opts.scrapers:
        print("Running all scrapers")
        df = _SCRAPER_REGISTRY.run_all_scrapers()
    else:
        print("Running selected scrapers")
        df = _SCRAPER_REGISTRY.run_scrapers(opts.scrapers)

    today = datetime.date.today()
    default_outputs = [f'covid_disparities_{today}.xlsx',
                       f'covid_disparities_{today}.csv']

    # Write output files
    for output in opts.outputs or default_outputs:
        write_output(df, output)


if __name__ == "__main__":
    main()
