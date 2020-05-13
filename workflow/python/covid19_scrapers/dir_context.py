from contextlib import contextmanager
import os


@contextmanager
def dir_context(path):
    """For use in a "with dir_context(path): ..." construction, this context
    manager changes directory to the specified path on entry and changes back
    to the prior cwd on exit."""
    oldcwd = os.getcwd()
    try:
        os.chdir(str(path))
        yield
    finally:
        print(f'{os.getcwd()} -> {oldcwd}')
        os.chdir(oldcwd)
