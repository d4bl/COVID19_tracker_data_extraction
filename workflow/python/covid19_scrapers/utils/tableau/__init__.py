__all__ = ['TableauParser', 'find_tableau_request']

from covid19_scrapers.utils.tableau.parser import TableauParser


def find_tableau_request(request):
    return 'bootstrapSession' in request.path
