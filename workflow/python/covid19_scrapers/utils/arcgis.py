import datetime

from arcgis.features import FeatureLayerCollection
from arcgis.gis import GIS


# Helpers for ESRI/ArcGIS web services (geoservices)
#
# The big idea is that the query parameters can be declared as a
# single class variable, and applied to the query_geoservice call.
#
# See, eg, states/missouri.py for examples.
def make_geoservice_stat(agg, in_field, out_name):
    """Make a single entry for the `stats` field of a query_geoservice
    request (a.k.a. the `outStatistics` field of a geoservice
    request).

    """
    return {
        'statisticType': agg,
        'onStatisticField': in_field,
        'outStatisticFieldName': out_name,
    }


def _get_layer_by_name(layer_name, layers, tables):
    """Find the named layer among either the layers or tables."""
    for layer in layers or []:
        if layer.properties.name == layer_name:
            return layer
    for table in tables or []:
        if table.properties.name == layer_name:
            return table


def _get_layer_by_id(layer_id, layers, tables):
    """Find the layer with the specified ID among either the layers or
    tables.

    """
    for layer in layers or []:
        if layer.properties.id == layer_id:
            return layer
    for table in tables or []:
        if table.properties.id == layer_id:
            return table


def _get_layer(flc_id, flc_url, layer_name):
    """Find the layer with the specified name (or ID, if integer) among
    either the layers or tables of the FeatureLayerCollection with the
    specified ID or URL.

    """
    # Get the feature layer collection.
    if flc_id:
        gis = GIS()
        flc = gis.content.get(flc_id)
        loc = f'content ID {flc_id}'
        assert flc is not None, f'Unable to find ArcGIS ID {flc_id}'
    elif flc_url:
        loc = f'flc URL {flc_url}'
        flc = FeatureLayerCollection(flc_url)
    else:
        raise ValueError('Either flc_id or url must be provided')

    # Now get the layer.
    if isinstance(layer_name, str):
        layer = _get_layer_by_name(layer_name, flc.layers, flc.tables)
    elif isinstance(layer_name, int):
        layer = _get_layer_by_id(layer_name, flc.layers, flc.tables)
    if layer:
        return layer
    raise ValueError(f'Unable to find layer {layer_name} in {loc}')


def query_geoservice(*, flc_id=None, flc_url=None, layer_name=None,
                     where='1=1', out_fields=['*'], group_by=None,
                     stats=None, order_by=None, limit=None):
    """Queries the specified ESRI GeoService.

    Mandatory arguments:
      Either of
        flc_id: FeatureLayerCollection ID to search for.
      or
        flc_url: URL for a FeatureServer or MapServer REST endpoint.
      and
        layer_name: the name or integer ID of the desired layer or table.
      must be provided.

    Optional arguments:
      where: the feature filtering query.
      out_fields: the fields to retrieve, defaults to all.
      group_by: the field by which to group for statistical operations.
      stats: a list of dicts specifying the desired statistical operations.
      order_by: field and direction to order by.
      limit: max number of records to retrieve.

    Returns: a pair consisting of the update date and data frame
      containing the features.
    """
    layer = _get_layer(flc_id, flc_url, layer_name)
    features = layer.query(
        spatialRel='esriSpatialRelIntersects',
        where=where,
        outFields=','.join(out_fields),
        return_geometry=False,
        groupByFieldsForStatistics=group_by,
        outStatistics=stats,
        orderByFields=order_by,
        resultRecordCount=limit,
        resultType='standard')
    try:
        update_date = datetime.datetime.fromtimestamp(
            layer.properties.editingInfo.lastEditDate / 1000).date()
    except AttributeError:
        update_date = None
    return update_date, features.sdf
