# -*- coding: utf-8 -*-
"""
Classes for downloading and working with Census API data.
"""
import os
import json
import collections
import re
import requests
import pandas as pd
from .util import check_response

CENSUS_BASE_URL = 'https://api.census.gov/data.json'


class CensusQuery(object):
    """
    Download data from the Census API.

    Requires an API key to run queries; see: http://api.census.gov/data/key_signup.html

    Usage is something like:
        q = CensusQuery('cachedir', 'ACSProfile5Y2015', api_key)
        results = q.query(['DP04_0045E', 'DP04_0045M'],
                          {'place':'*'},
                          {'state':'06', 'county':'037'})
    """
    def __init__(self, cache_dir, dataset_name, api_key, verbose=False):
        self.cache_dir = cache_dir
        self.dataset_name = dataset_name
        self.api_key = api_key
        self.verbose = verbose
        self.level = ''
        self.inclause = ''
        self.dataset = None
        self.vars = None

        if not self.cache_dir:
            raise ValueError('Must specify valid cache_dir')
        elif not os.path.exists(self.cache_dir):
            os.mkdir(self.cache_dir)
            if self.verbose:
                print('Create dir [{}]'.format(self.cache_dir))
        elif not os.path.isdir(self.cache_dir):
            raise OSError('Invalid cache dir [{}]'.format(self.cache_dir))

        self.query_cache = _QueryCache(self.cache_dir, verbose=self.verbose)
        self._load_cache()
        if not self.dataset:
            self._fetch_dataset()
        if not self.vars:
            self._fetch_vars()

    def search_vars(self, pat):
        """
        Find census variables that match a pattern.

        Args:
            pat: regular expression

        Returns:
            list of strings
        """
        rev = re.compile(pat)
        return [k for k in self.vars.keys() if rev.search(k)]

    def get_vars(self, ids=[]):
        """
        Get info for the given variables.
        Args:
            list of variable names
        Returns:
            dict of variable names to info dicts
        """
        return {
            k:self.vars[k]
            for k in self.vars.keys()
            if k in ids
        }

    # https://api.census.gov/data/2015/acs5/examples.html
    def query(self, get_clause=[], for_clause={}, in_clause={}, force=False):
        """
        Query the Census API.

        Args:
            get_clause (list): names of variables to fetch.
            for_clause (dict): filter criteria.
            in_clause (dict): specifiers for the for clause.

        Returns:
            DataFrame
        """

        # transform predicates to formatted strings
        _for = ' '.join(
            '{}:{}'.format(k, v)
            for k, v in list(for_clause.items()))
        _in = ' '.join(
            '{}:{}'.format(k, v)
            for k, v in list(in_clause.items()))

        # make sure we always get name and geoid
        if 'GEOID' not in get_clause:
            get_clause.append('GEOID')
        if 'NAME' not in get_clause:
            get_clause.append('NAME')
        _get = ','.join(sorted(get_clause))

        if self.verbose:
            print('get [{}]'.format(_get))
            print('for [{}]'.format(_for))
            print('in  [{}]'.format(_in))

        url = self.dataset['distribution'][0]['accessURL']
        parms = {'get':_get, 'for':_for, 'key':self.api_key,}
        if _in:
            parms['in'] = _in

        # check the cache
        cachekey = self.query_cache.make_key(url, parms)
        if not force:
            df, cachefn = self.query_cache.load(cachekey)
            if df is not None:
                if self.verbose:
                    print('Query results from cache [{}] [{}]'.format(cachefn, df.shape))
                return df

        # run the query already!
        resp = requests.get(url, parms)
        check_response(resp)
        if self.verbose:
            print('Query request ok')

        # transform output to a DataFrame
        ja = resp.json()
        df = pd.DataFrame(ja[1:], columns=ja[0])

        # strip the weird prefix on geoid
        #df['GEOID'] = df['GEOID'].str.replace('^.*US', '')

        # convert columns to numeric where indicated by the variable spec
        varlist = self.get_vars(get_clause)
        for vid, var in varlist.items():
            if 'predicateType' in var and var['predicateType'] == 'int':
                try:
                    df[vid] = pd.to_numeric(df[vid])
                except ValueError as err:
                    if self.verbose:
                        print('Can\'t convert column [{}] to numeric: [{}]'.format(vid, err))
        print(df.dtypes)

        self.query_cache.save(cachekey, df)
        return df

    # return valid filter sets (the 'in' clause of a query)
    # for a given geo level (the 'for' clause of a query)
    def geo_parameter_chart(self):
        """
        Formats the current dataset's geography chart into a dict that makes it
        a little easier to see what the valid `in_clause`s are for a given
        `for_clause` in `query()`.

        Returns:
            OrderedDict: dict of lists of strings

        Reference:
            https://api.census.gov/data/2015/acs5/geography.json
        """
        resp = requests.get(self.dataset['c_geographyLink'])
        check_response(resp)

        # pivot data into something we can sort
        dgeo = {}
        for i in resp.json()['fips']:
            dgeo[i['geoLevelId']] = i

        # iterating in ID order and inserting the first instance of each name
        # gets us something close to a hierarchy
        rv = collections.OrderedDict()
        for i in sorted(dgeo.keys()):
            geo = dgeo[i]
            if not geo['name'] in rv:
                rv[geo['name']] = []
            rv[geo['name']].append(geo['requires'] if 'requires' in geo else [])

        return rv

    # ------------------------------------------------------

    def _load_cache(self):
        fn = os.path.join(self.cache_dir, '%s_dataset.json'%self.dataset_name)
        if os.path.exists(fn):
            with open(fn) as fp:
                self.dataset = json.load(fp)
                if self.dataset and self.verbose:
                    print('Dataset from cache [{}] [{}]'.format(fn, self.dataset['title']))
        fn = os.path.join(self.cache_dir, '%s_vars.json'%self.dataset_name)
        if os.path.exists(fn):
            with open(fn) as fp:
                self.vars = json.load(fp)
                if self.vars and self.verbose:
                    print('Vars from cache [{}] [{}]'.format(fn, len(self.vars)))

    def _fetch_dataset(self):
        resp = requests.get(CENSUS_BASE_URL)
        check_response(resp)
        self.dataset = [
            d for d in resp.json()['dataset']
            if self.dataset_name in d['identifier']
        ][0]
        if self.verbose:
            print('Query dataset ok [{}]'.format(self.dataset['title']))
        fn = os.path.join(self.cache_dir, '%s_dataset.json'%self.dataset_name)
        with open(fn, 'w') as fp:
            json.dump(self.dataset, fp, indent=1)
            if self.verbose:
                print('Saved dataset to cache [{}]'.format(fn))

    def _fetch_vars(self):
        url = self.dataset['c_variablesLink']
        resp = requests.get(url)
        check_response(resp)
        j = resp.json()
        self.vars = j['variables']
        if self.verbose:
            print('Query vars ok [{}]'.format(len(self.vars)))
        fn = os.path.join(self.cache_dir, '%s_vars.json'%self.dataset_name)
        with open(fn, 'w') as fp:
            json.dump(self.vars, fp, indent=1)
            if self.verbose:
                print('Saved vars to cache [{}]'.format(fn))


class _QueryCache(object):
    """
    Manage the cache of previous query results.
    """
    def __init__(self, cache_dir, verbose=False):
        self.verbose = verbose
        self.cache_dir = cache_dir
        self.index = {}
        self.index_fn = os.path.join(self.cache_dir, 'query_index.json')
        if os.path.exists(self.index_fn):
            with open(self.index_fn) as fp:
                self.index = json.load(fp)
                if self.index and self.verbose:
                    print('Query index from cache [{}] [{}]'.format(self.index_fn, len(self.index)))

        self.last_fni = 1001
        if self.index:
            self.last_fni = max(self.index.values())

    def make_key(self, url, parms):
        """
        Make an index key from a requests url+parameters.
        """
        # sort keys to ensure consistency
        # and be sure not to include the api key!
        p2 = collections.OrderedDict([(k, parms[k]) for k in sorted(parms.keys()) if k != 'key'])
        req = requests.Request(method='GET', url=url, params=p2)
        pr = req.prepare()
        return pr.url

    def _itofn(self, i):
        return os.path.join(self.cache_dir, 'qc{:09d}.csv'.format(i))

    def _fntoi(self, fn):
        bn = os.path.basename(fn)
        i = re.sub(r'\D', '', bn)
        return int(i)

    def load(self, key):
        """
        Args:
            key: a query url prodced by make_key()
        Returns:
            DataFrame, string: the cached data and the filename it was stored
            at. If the cache misses, DataFrame will be null.
        """
        if key not in self.index:
            return None, ''

        fni = self.index[key]
        fn = self._itofn(fni)
        if not os.path.exists(fn):
            if self.verbose:
                print('WARNING: Query index cache file [{}] missing for key [{}]'.format(fn, key))
            del self.index[key]
            return None, fn

        df = pd.read_csv(fn)
        return df, fn

    def save(self, key, df):
        """
        Args:
            key: a query url prodced by make_key()
            df: a DataFrame to be saved
        """
        fni = self.last_fni + 1
        if key in self.index:
            # overwrite existing cache entry
            fni = self.index[key]
        fn = self._itofn(fni)
        df.to_csv(fn)
        self.index[key] = fni
        self.last_fni = max(self.last_fni, fni)
        with open(self.index_fn, 'w') as fp:
            json.dump(self.index, fp, indent=1)
        if self.verbose:
            print('Query results cached to [{}]'.format(fn))

