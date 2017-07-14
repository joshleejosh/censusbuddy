# -*- coding: utf-8 -*-
"""
Classes and functions to make querying Census and TIGER data a little easier.

The main points of interaction are CensusQuery and TigerDownloaderHTTP.

Example:
    # Query census data
    cenquery = censusbuddy.CensusQuery('cache/census',
                                       'ACSProfile5Y2015',
                                       API_KEY)
    cendf = cenquery.query(['DP05_0001E', 'DP05_0001M'],
                           {'place':'*'},
                           {'state':'06'})
    print(cendf.shape)

    # Query tiger data
    geoquery = censusbuddy.TigerDownloaderHTTP('cache/tiger')
    geodf = geoquery.query(2015, '06', 'place', '500k')
    print(geodf.shape)

    # Merge the two datasets
    mergedf = censusbuddy.merge_frames(cendf, geodf)
    print(mergedf.shape)
    print(mergedf.columns)
    print(mergedf.sample(10))

"""
import pandas
import geopandas

from .census import CensusQuery
from .tiger import TigerDownloaderFTP
from .tiger import TigerDownloaderHTTP
from .tiger import TigerREST

def merge_frames(cendf, geodf):
    """
    Merge a census frame (presumably coming from a CensusQuery)
    with a geo frame (presumably coming from a TigerDownloader).

    Assume that both frames have a GEOID column to join on.
    """

    # census data comes with weird prefixes on geoid, so strip it
    cendf['GEOID'] = cendf['GEOID'].str.replace(r'^.*US', '')

    # frames probably have duplicate NAME columns; keep the one from tiger.
    pdf = pandas.merge(cendf, geodf, on='GEOID', suffixes=('_DELETEME', ''))
    for col in pdf.columns:
        if col.endswith('_DELETEME'):
            del pdf[col]
    gdf = geopandas.GeoDataFrame(pdf)
    return gdf


def strip_prefix(series):
    """
    Strip any shared prefix off of a series of strings.

    Sometimes comes in handy for the junk prefixes on GEOIDs.
    """
    prefix = ''
    maxlen = 100
    # find the longest starting string that all the values have in common.
    for i in range(maxlen):
        vals = None
        try:
            vals = series.str[i]
        except IndexError:
            break
        if vals is None or vals.empty:
            break
        valset = set(vals.values)
        if len(valset) > 1:
            break
        prefix += list(valset)[0]
    return series.str.replace(prefix, '')

