import datetime
from io import BytesIO
from zipfile import ZipFile

from covid19_scrapers.utils.http import get_content_as_file


# Wrappers to handle zip files
def unzip(path_to_zip_file, directory_to_extract_to='.'):
    """Unzip a zip file by path to a directory, by default the working
    directory.
    """
    with ZipFile(path_to_zip_file, 'r') as zip_ref:
        zip_ref.extractall(directory_to_extract_to)


def get_zip(url):
    """Fetch a zip file by URL and return a ZipFile object to access its
    contents and metadata.
    """
    return ZipFile(get_content_as_file(url))


def get_zip_member_as_file(zipfile, path, mode='r'):
    """Given a ZipFile object, retrieve one of its members as a filelike.
    """
    return BytesIO(zipfile.read(path))


def get_zip_member_update_date(zipfile, path, mode='r'):
    """Given a ZipFile object and member name, retrieve the member's
    timestamp as a date.
    """
    (year, month, date, h, m, s) = zipfile.getinfo(path).date_time
    return datetime.date(year, month, date)
