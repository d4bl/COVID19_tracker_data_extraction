import inspect
import json
from pathlib import Path

import jinja2


def get_template(template_name):
    current_file = inspect.getfile(get_template)
    template_path = '/'.join(current_file.split('/')[:-1] + ['templates'])
    _loader = jinja2.FileSystemLoader(searchpath=template_path)
    _env = jinja2.Environment(loader=_loader, autoescape=True)
    return _env.get_template(template_name)


def get_json(file_name):
    current_file = inspect.getfile(get_json)
    base_path = Path('/'.join(current_file.split('/')[:-1] + ['json']))
    json_path = Path(file_name)
    with base_path.joinpath(json_path).open() as json_file:
        return json.loads(json_file.read())
