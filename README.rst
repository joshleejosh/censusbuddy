###########
CENSUSBUDDY
###########

A module to help make querying Census and TIGER data a little quicker and
easier.


REQUIREMENTS
============

Python packages:

* requests
* pandas
* geopandas

Installed on your machine:

* GDAL <http://www.gdal.org/> (specifically, ``TigerDownloader.unpack()`` does
  a system callout to ``ogr2ogr``)

A Census API key (see <http://api.census.gov/data/key_signup.html>)

EXAMPLE USAGE
=============

.. code:: python

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

    # Merge the two datasets into a single GeoDataFrame
    mergedf = censusbuddy.merge_frames(cendf, geodf)
    print(mergedf.shape)
    print(mergedf.columns)
    print(mergedf.sample(10))

