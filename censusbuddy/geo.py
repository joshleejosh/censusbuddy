# -*- coding: utf-8 -*-
"""
Helpers for looking up and cross-referencing geo data labels.
"""

import os.path
import re
import pandas as pd

_REFDIR = os.path.join(os.path.dirname(__file__), '..', 'data')


# ==========================================================


class _GeoRef(object):
    """
    A return value from entity_to_census().

    Attributes:
        tiger
        census
    """
    def __init__(self, t, c):
        self.tiger = t
        self.census = c
    def __repr__(self):
        return '_GeoRef("{}","{}")'.format(self.tiger, self.census)
    def __str__(self):
        return self.__repr__()

_ENTCEN_XREF = None

def entity_to_census(entity):
    """
    Look up the (rough?) Census API equivalent to a given TIGER entity.

    Args:
        entity: TIGER file entity code.

    Reference:
        TIGER entities: https://www2.census.gov/geo/tiger/GENZ2016/2016_file_name_def.pdf
        Census geography: https://api.census.gov/data/2015/acs5/profile/geography.json
    """
    global _ENTCEN_XREF
    if _ENTCEN_XREF is None:
        _ENTCEN_XREF = pd.read_csv(os.path.join(_REFDIR, 'geo_xref.csv'), dtype=str)

    candidates = _ENTCEN_XREF[_ENTCEN_XREF['tiger'] == entity]['census']
    if candidates.shape[0] == 0:
        raise KeyError('Could not find labels matching [{}]'.format(entity))
    if candidates.shape[0] > 1:
        print('WARNING: multiple labels found for [{}]: [{}]'.format(entity, candidates))
    return candidates.values[0]


# ==========================================================

_FIPS_STATE = None
_FIPSFN_STATE = os.path.join(_REFDIR, 'fips_state.csv')
_FIPS_REVERSE = None
_FIPS_COUNTY = None
_FIPSFN_COUNTY = os.path.join(_REFDIR, 'fips_county.csv')
_FIPS_COUSUB = None
_FIPSFN_COUSUB = os.path.join(_REFDIR, 'fips_cousub.csv')

def _chk_candidates(candidates, typ, key):
    """
    Check the results of a search in state_fips(), county_fips(), or
    cousub_fips().
    Raises:
        KeyError if the result set is empty.
    """
    if not candidates:
        raise KeyError('Could not find {} matching [{}]'.format(typ, key))
    if len(candidates) > 1:
        print('WARNING: multiple matches for [{}]: [{}]'.format(key, candidates))

def state_fips(pat):
    """
    Look up the FIPS code for a state.

    Args:
        pat: a state postal code or name. Can be partial or a regex.

    Returns:
        string: FIPS code for the state.

    Raises:
        KeyError if no code is found.

    Reference:
        https://www.census.gov/geo/reference/ansi_statetables.html
    """
    global _FIPS_STATE
    if _FIPS_STATE is None:
        _FIPS_STATE = pd.read_csv(_FIPSFN_STATE, dtype=str)

    rp = re.compile(pat, re.I)
    candidates = []
    for _, row in _FIPS_STATE.iterrows():
        if rp.match(row['STATE']) or rp.match(row['STUSAB']) or rp.match(row['STATE_NAME']):
            candidates.append(row['STATE'])
    _chk_candidates(candidates, 'states', pat)
    return candidates[0]


def fips_state(fips):
    """
    Get the postal code for a state FIPS code.

    Args:
        fips (string): FIPS code for a state.

    Returns:
        string: 2-letter postal code.
    """
    global _FIPS_REVERSE, _FIPS_STATE
    if _FIPS_REVERSE is None:
        if _FIPS_STATE is None:
            _FIPS_STATE = pd.read_csv(_FIPSFN_STATE, dtype=str)
        _FIPS_REVERSE = dict(zip(_FIPS_STATE['STATE'], _FIPS_STATE['STUSAB']))
    return _FIPS_REVERSE[fips]


def county_fips(state, pat):
    """
    Look up the FIPS code for a county within a state.

    Args:
        state: FIPS code or postal code for a state. (No partials)
        pat: name of a county in the state. Case insensitive. Can be partial or regex.

    Returns:
        string: FIPS code for the county.

    Raises:
        KeyError if no code is found.

    Reference:
        https://www.census.gov/geo/reference/codes/cou.html
    """
    global _FIPS_COUNTY
    rp = re.compile(pat, re.I)
    candidates = []
    if _FIPS_COUNTY is None:
        _FIPS_COUNTY = pd.read_csv(_FIPSFN_COUNTY, dtype=str)

    for _, row in _FIPS_COUNTY.iterrows():
        if row['STATEFP'] != state and row['STATE'] != state:
            continue
        if rp.match(row['COUNTYFP']) or rp.match(row['COUNTYNAME']):
            candidates.append(row['COUNTYFP'])
    _chk_candidates(candidates, 'counties', '{}/{}'.format(state, pat))
    return candidates[0]

def cousub_fips(state, county, pat):
    """
    Look up the FIPS code for a county subdivision.

    Args:
        state: FIPS code or postal code for a state. (No partials)
        county: FIPS code for a county. (No partials)
        pat: name of a subdivision. Case insensitive. Can be partial or regex.

    Returns:
        string: FIPS code for the subdivision.

    Raises:
        KeyError if no code is found.

    Reference:
        https://www.census.gov/geo/reference/codes/cousub.html
    """
    global _FIPS_COUSUB
    rp = re.compile(pat, re.I)
    candidates = []
    if _FIPS_COUSUB is None:
        _FIPS_COUSUB = pd.read_csv(_FIPSFN_COUSUB, dtype=str)

    if isinstance(county, int):
        county = '{:03d}'.format(county)

    for _, row in _FIPS_COUSUB.iterrows():
        if row['STATEFP'] != state and row['STATE'] != state:
            continue
        if row['COUNTYFP'] != county:
            continue
        if rp.match(row['COUSUBFP']) or rp.match(row['COUSUBNAME']):
            candidates.append(row['COUSUBFP'])

    _chk_candidates(candidates, 'cousubs', '{}/{}/{}'.format(state, county, pat))
    return candidates[0]

