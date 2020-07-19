import enum
import inspect
import json
from pathlib import Path

import jinja2
import pandas as pd


class FileType(enum.Enum):
    CSV = 'csv'
    JSON = 'json'
    BLOB = 'blob'
    TEMPLATE = 'template'


def get_path(file_type, file_name):
    current_file = inspect.getfile(get_path)
    base_path = Path('/'.join(current_file.split('/')[:-1] + [file_type.value]))
    file_path = Path(file_name)
    return base_path.joinpath(file_path)


def get_csv(csv_file, **kwargs):
    csv_path = get_path(FileType.CSV, csv_file)
    return pd.read_csv(str(csv_path), **kwargs)


def get_template(template_name):
    current_file = inspect.getfile(get_template)
    template_path = '/'.join(current_file.split('/')[:-1] + ['templates'])
    _loader = jinja2.FileSystemLoader(searchpath=template_path)
    _env = jinja2.Environment(loader=_loader, autoescape=True)
    return _env.get_template(template_name)


def get_blob(file_name):
    path = get_path(FileType.BLOB, file_name)
    with open(path, 'r') as file:
        blob = file.read()
    return blob


def get_json(file_name):
    file = get_path(FileType.JSON, file_name)
    with file.open() as json_file:
        return json.loads(json_file.read(), object_hook=try_keys_to_int)


def try_keys_to_int(d):
    try:
        return {int(k): v for k, v in d.items()}
    except ValueError:
        return d
