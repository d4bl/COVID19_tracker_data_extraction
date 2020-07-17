import inspect
import jinja2


def get_template(template_name):
    current_file = inspect.getfile(get_template)
    template_path = '/'.join(current_file.split('/')[:-1])
    _loader = jinja2.FileSystemLoader(searchpath=template_path)
    _env = jinja2.Environment(loader=_loader, autoescape=True)
    return _env.get_template(template_name)
