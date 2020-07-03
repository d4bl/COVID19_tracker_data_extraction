"""This imports all the state scrapers by file, to avoid having to
maintain a list manually. This is to avoid a wildcard import in the
parent __init__.py

"""
import importlib
from pathlib import Path
import re

__all__ = []

for f in Path(__file__).parent.glob('*.py'):
    if f.is_file() and not re.search(r'__init__.py$|flymake|test', f.name):
        module = importlib.import_module(
            f'covid19_scrapers.states.{f.name[:-3]}')
        __all__.append(f.name[:-3])
