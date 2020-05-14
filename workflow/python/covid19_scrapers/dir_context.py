from contextlib import contextmanager
import logging
import os


_logger = logging.getLogger(__name__)


@contextmanager
def dir_context(path):
    """This context manager changes directory to path on entry, and
    changes back to the prior cwd on exiting the scope.  For use in a
       "with dir_context(path): ..."
    construction.

    path: required, string or Pathlike for a directory that exists.
    """
    oldcwd = os.getcwd()
    try:
        _logger.debug(f'Entering: {oldcwd} -> {path}')
        os.chdir(str(path))
        yield
    finally:
        _logger.debug(f'Exiting: {os.getcwd()} -> {oldcwd}')
        os.chdir(oldcwd)
