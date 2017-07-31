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

    Usage looks something like:
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
    def query(self, get_clause=[], for_clause={}, in_clause={}, parameters={}, cache=True):
        """
        Query the Census API.

        Args:
            get_clause (list): names of variables to fetch.
            for_clause (dict): geo filter criteria.
            in_clause (dict): specifiers for the for clause.
            parameters (dict): other parameters.

        Returns:
            DataFrame

        Example:
            cendf = cenquery.query(['DP05_0001E', 'DP05_0001M'],
                                   {'place':'*'},
                                   {'state':'06'})
        """

        # transform predicates to formatted strings
        if for_clause and in_clause:
            if not self.validate_predicate(for_clause, in_clause):
                return pd.DataFrame()
        _for = ''
        if for_clause:
            _for = ' '.join(
                '{}:{}'.format(k, v)
                for k, v in list(for_clause.items()))
        _in = ''
        if in_clause:
            _in = ' '.join(
                '{}:{}'.format(k, v)
                for k, v in list(in_clause.items()))

        # make sure we always get name and geoid (if appropriate)
        if 'GEOID' not in get_clause and self.get_vars(['GEOID']):
            get_clause.append('GEOID')
        if 'NAME' not in get_clause and self.get_vars(['NAME']):
            get_clause.append('NAME')
        _get = ','.join(sorted(get_clause))

        # build the parameter set
        parms = {}
        parms.update(parameters)
        if _get:
            parms['get'] = _get
        if _for:
            parms['for'] = _for
        if _in:
            parms['in'] = _in
        parms['key'] = self.api_key
        if self.verbose:
            print('\n'.join('\t{} [{}]'.format(k, parms[k])
                            for k in sorted(parms.keys())
                            if k != 'key'))

        url = self.dataset['distribution'][0]['accessURL']

        # check the cache
        if cache:
            cachekey = self.query_cache.make_key(url, parms)
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

        # convert columns to numeric where indicated by the variable spec
        varlist = self.get_vars(get_clause)
        for vid, var in varlist.items():
            if 'predicateType' in var and var['predicateType'] == 'int':
                try:
                    df[vid] = pd.to_numeric(df[vid])
                except Exception as err:
                    if self.verbose:
                        print('Can\'t convert column [{}] to numeric: [{}]'.format(vid, err))

        if cache:
            self.query_cache.save(cachekey, df)
        return df

    def validate_predicate(self, for_clause, in_clause):
        """
        Make sure that the given in_clause is valid for the for_clause.
        Returns:
            bool
        """
        resp = requests.get(self.dataset['c_geographyLink'])
        check_response(resp)
        ins = in_clause.keys()
        db = resp.json()['fips']

        for fori in for_clause.keys():
            # Gather requirement options for the 'for' clause.
            reqs = [
                'requires' in rec and rec['requires'] or []
                for rec in db
                if rec['name'] == fori
                ]

            # Check each combination of requirements; if any of them
            # match our "in" clause, we're ok.
            forok = False
            for j in reqs:
                if sorted(j) == sorted(ins):
                    forok = True
                    break

            if not forok:
                print('ERROR: for clause [{}] won\'t work with in clause [{}]'.format(for_clause, in_clause))
                print('    Try one of these combinations for the in clause:')
                for j in reqs:
                    print('        {}'.format(', '.join(j)))
                return False

        return True

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

