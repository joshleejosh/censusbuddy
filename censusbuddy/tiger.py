# -*- coding: utf-8 -*-
"""
Classes for downloading and converting TIGER shape data.
"""
import os
import tempfile
import zipfile
import shutil
import subprocess
from ftplib import FTP
import requests
import pandas as pd
import geopandas as gpd
import shapely
from .util import check_response

# ------------------------------------------------------------------------------

class TigerDownloader(object):
    """
    Download Cartographic Boundary shapefiles from TIGERweb.

    This is an abstract class; instantiate TigerDownloaderHTTP or
    TigerDownloaderFTP to do real work.

    Usage is something like:
        q = TigerDownloaderHTTP('cachedir')
        q.configure(2015, '06', 'place', '500k')
        q.fetch()
        results = q.unpack()
    """

    _HTTP_SERVER = 'www2.census.gov'
    _FTP_SERVER = 'ftp2.census.gov'
    _BASEDIR = '/geo/tiger/GENZ{}/shp'

    def __init__(self, cache_dir, dryrun=False, verbose=False):
        self.content_type = 'cb' # Cartographic Boundary files, as opposed to 'tl' for TIGER/Line
        self.dryrun = dryrun
        self.verbose = verbose
        self.cache_dir = cache_dir

        if not self.cache_dir:
            raise ValueError('Must specify valid cache_dir')
        elif not os.path.exists(self.cache_dir):
            if not self.dryrun:
                os.mkdir(self.cache_dir)
                if self.verbose:
                    print('create dir [{}]'.format(self.cache_dir))
        elif not os.path.isdir(self.cache_dir):
            raise OSError('Invalid cache dir [{}]'.format(self.cache_dir))

        self.configure(2016, 'us', 'nation', '20m')

    def query(self, year, state, entity, resolution, simplify=0, cache=True):
        """
        Wraps the configure() -> fetch() -> unpack() sequence.
        """
        self.configure(year, state, entity, resolution)
        self.fetch(cache=cache)
        return self.unpack(simplify=simplify, cache=cache)

    def configure(self, year, state, entity, resolution):
        """
        Set query parameters and put together a filename.

        Filenames look something like:
            cb_2014_28_place_500k.zip
            cb_2015_us_county_20m.zip
            cb_2016_34_puma10_500k.zip

        See: https://www2.census.gov/geo/tiger/
        """
        self.year = str(year)
        self.state = str(state) # fips code, or 'us' for national
        self.entity = str(entity)
        self.resolution = str(resolution) # probably 20m, 5m, or 500k
        self.basename = '_'.join((self.content_type, self.year, self.state,
                                  self.entity, self.resolution))
        return self.basename

    def list(self):
        """
        Print a list of available files in the current directory. Must set this
        up with `configure()` first.
        """
        raise NotImplementedError()

    def fetch(self, cache=True):
        """
        Download a zip file from the TIGER site. Must set this up with
        `configure()` first.

        If the zip already exists in the cache dir, this will be skipped.
        """
        raise NotImplementedError()

    def unpack(self, simplify=0, cache=True):
        """
        Unpack TIGER Shapefiles from a zipped bundle and convert to GeoJSON.

        This works by calling out to `ogr2ogr`, so make sure you have GDAL
        installed.

        Returns the GeoJSON data in a GeoDataFrame.

        If the GeoJSON already exists in the cache dir, this will be skipped.
        """

        jfn = os.path.join(self.cache_dir, self.basename + '.geojson')
        if cache and os.path.exists(jfn):
            if self.verbose:
                print('GeoJSON file [{}] exists, skipping conversion'.format(jfn))
            return gpd.read_file(jfn)

        tempdir = tempfile.mkdtemp()
        if self.verbose:
            print('temp dir: [{}]'.format(tempdir))
        try:
            # unzip into the tempdir
            zfn = os.path.join(self.cache_dir, self.basename + '.zip')
            with zipfile.ZipFile(zfn) as zfp:
                zfp.extractall(tempdir)
                if self.verbose:
                    print('unpacked: [{}]'.format(os.listdir(tempdir)))

            sfn = os.path.join(tempdir, self.basename + '.shp')

            # Run ogr2ogr externally
            if not shutil.which('ogr2ogr'):
                print('ERROR: ogr2ogr not installed')
                return gpd.GeoDataFrame()
            args = ['ogr2ogr']
            if simplify > 0:
                args.extend(('-simplify', str(simplify)))
            args.extend(('-f', 'GeoJSON'))
            args.extend(('-t_srs', 'crs:84'))
            args.append(jfn)
            args.append(sfn)
            srv = subprocess.call(args)
            if srv != 0:
                print('ERROR: ogr2ogr failed: [{}]'.format(srv))
                return gpd.GeoDataFrame()
            else:
                if self.verbose:
                    print('converted: [{}]'.format(os.path.getsize(jfn)))

            return gpd.read_file(jfn)

        finally:
            try:
                shutil.rmtree(tempdir)
            except OSError:
                pass


# ------------------------------------------------------------------------------

class TigerDownloaderHTTP(TigerDownloader):
    """
    Download Tiger geo outlines via HTTP from `www2.census.gov`
    """

    def __init__(self, cache_dir, dryrun=False, verbose=False):
        TigerDownloader.__init__(self, cache_dir, dryrun, verbose)

    def list(self):
        bd = TigerDownloader._BASEDIR.format(self.year)
        baseurl = 'https://{}{}'.format(TigerDownloader._HTTP_SERVER, bd)
        print('TODO: file listing, see [{}]'.format(baseurl)) # scrape the index page I guess?

    def fetch(self, cache=True):
        ifn = self.basename + '.zip'
        ofn = os.path.join(self.cache_dir, self.basename + '.zip')
        if cache and os.path.exists(ofn):
            if self.verbose:
                print('Zip file [{}] exists, skipping download'.format(ofn))
            return ofn

        bd = TigerDownloader._BASEDIR.format(self.year)
        url = 'https://{}{}/{}'.format(TigerDownloader._HTTP_SERVER, bd, ifn)

        if self.verbose:
            print('fetch [{}] -> [{}]'.format(url, ofn))
        if self.dryrun:
            return ofn

        resp = requests.get(url, stream=True)
        if resp.status_code == 200:
            with open(ofn, 'wb') as fp:
                for chunk in resp:
                    fp.write(chunk)
        else:
            print('Aborting: [{}]'.format(resp.status_code))
        return ofn

# ------------------------------------------------------------------------------

class TigerDownloaderFTP(TigerDownloader):
    """
    Download Tiger geo outlines via FTP from `ftp2.census.gov`
    """

    def __init__(self, cache_dir, dryrun=False, verbose=False):
        TigerDownloader.__init__(self, cache_dir, dryrun, verbose)
        self.ftp = None

    def _login(self):
        if self.dryrun:
            return
        if not self.ftp:
            self.ftp = FTP(TigerDownloader._FTP_SERVER)
            self.ftp.login()
        path = TigerDownloader._BASEDIR.format(self.year)
        if self.ftp.pwd() != path:
            self.ftp.cwd(path)
        if self.verbose:
            print('cwd to [{}]'.format(self.ftp.pwd()))

    def list(self):
        arg = '*'
        if self.verbose:
            print('listing [{}]'.format(arg))
        if not self.dryrun:
            self._login()
            self.ftp.dir(arg)

    def fetch(self, cache=True):
        ifn = self.basename + '.zip'
        ofn = os.path.join(self.cache_dir, self.basename + '.zip')
        if cache and os.path.exists(ofn):
            print('Zip file [{}] exists, skipping download'.format(ofn))
            return ofn

        command = 'RETR {}'.format(ifn)
        if self.verbose:
            print('fetch [{}] -> [{}]'.format(command, ofn))

        if not self.dryrun:
            self._login()
            self.ftp.retrbinary(command, open(ofn, 'wb').write)
        return ofn



# ##############################################################################
# ##############################################################################
# ##############################################################################

class TigerREST(object):
    """
    Query the TIGERweb REST API for geo data.

    This class does work, but you probably want to use TigerDownloader instead:
    the API's query structure is finicky, and when it does work, the returned
    json data is large and slow to download. Also, I'm not sure I'm
    transforming the resulting geo correctly (some multipolygons seem to go
    missing, and lord knows if I'm doing winding right). Also also, the
    returned geo is TIGER/Line instead of Cartographic Boundary, so you get a
    lot of unwanted water coverage.
    """

    _BASE_URL = 'http://tigerweb.geo.census.gov/arcgis/rest/services/TIGERweb'

    def __init__(self, svcid, verbose=False):
        self.service = svcid
        self.verbose = verbose

    def query(self, layer, filt):
        """
        Args:
            layer (int): numeric map layer ID. See https://tigerweb.geo.census.gov/arcgis/rest/services/TIGERweb/tigerWMS_ACS2015/MapServer
            filt (dict): map of filter types and values.
        Returns:
            GeoDataFrame
        """
        layer = str(layer)

        where = ''
        if isinstance(filt, str):
            where = filt
        elif isinstance(filt, dict):
            where = ' and '.join('{}={}'.format(k, v) for k, v in filt.items())
        if not where:
            print('WARNING: no where clause')
        if self.verbose:
            print('layer [{}]'.format(layer))
            print('where [{}]'.format(where))
        qr = self._query(layer, where)
        rv = self._transform(qr)
        return rv

    # cf. https://tigerweb.geo.census.gov/arcgis/rest/services/TIGERweb/tigerWMS_ACS2015/MapServer
    def _query(self, layer, where):
        parms = {
            'f':'json',
            'where':where,
            'geometryType':'esriGeometryEnvelope',
            'outfields':'*',

            'returnGeometry':'true',
            'returnTrueCurves':'false',
            'returnIdsOnly':'false',
            'returnCountOnly':'false',
            'returnZ':'false',
            'returnM':'false',
            'returnDistinctValues':'false',

            #'text':'',
            #'objectIds':'',
            #'time':'',
            #'geometry':'',
            #'inSR':'',
            #'spatialRel':'esriSpatialRelIntersects',
            #'relationParam':'',
            #'maxAllowableOffset':'',
            #'geometryPrecision':'',
            #'outSR':'',
            #'orderByFields':'',
            #'groupByFieldsForStatistics':'',
            #'outStatistics':'',
            #'gdbVersion':'',
            #'resultOffset':'',
            #'resultRecordCount':'',
        }

        url = '{}/{}/MapServer/{}/query'.format(TigerREST._BASE_URL, self.service, layer)
        resp = requests.get(url, parms)
        check_response(resp)
        if self.verbose:
            print('query geo ok')
        rv = resp.json()
        return rv

    def _transform(self, gdata):
        # We've made some assumptions about the data; warn if they break.
        if 'geometryType' not in gdata:
            print('WARNING: no geometryType in data')
        if gdata['geometryType'] != 'esriGeometryPolygon':
            print('WARNING: geometryType is [{}], was expecting [esriGeometryPolygon]'.format(gdata['geometryType']))
        if 'spatialReference' not in gdata:
            print('WARNING: no spatialReference in data')
        if 'latestWkid' not in gdata['spatialReference'] or gdata['spatialReference']['latestWkid'] != 3857:
            print('WARNING: spatialReference is [{}], was expecting [3857]'.format(gdata['spatialReference']))

        # twist feature data into a GeoDataFrame
        features = []
        for feat in gdata['features']:
            fd = {}
            fd.update(feat['attributes'])

            # Convert ESRI ring objects into a GeoJSON MultiPolygon
            mp = []
            if 'geometry' in feat and 'rings' in feat['geometry']:
                for ring in feat['geometry']['rings']:
                    mp.append(shapely.geometry.Polygon(ring))
            fd['geometry'] = shapely.geometry.MultiPolygon(mp)

            features.append(fd)

        df = pd.DataFrame(features)
        rv = gpd.GeoDataFrame(df)

        # Convert coordinate systems:
        #     EPSG 3857 is projected (Web Mercator)
        #     EPSG 4326 is lat/long (so... unprojected?)
        # Leaflet prefers 4326, so let's go with that.
        rv.crs = {'init':'epsg:3857'}
        rv.to_crs(crs={'init':'epsg:4326'}, inplace=True)

        return rv

