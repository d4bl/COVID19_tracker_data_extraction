__all__ = ['TableauParser', 'find_tableau_request']


from covid19_scrapers.utils.tableau.parser import TableauParser


def find_tableau_request(request):
    if '/bootstrapSession/sessions/' not in request.path:
        return False
    if request.response and request.response.body:
        return 'sheetName' in request.response.body.decode('utf8')
    return False
