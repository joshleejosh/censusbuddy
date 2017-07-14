import unittest
import censusbuddy

class TigerRESTTest(unittest.TestCase):
    def test_rest(self):
        service = 'tigerWMS_ACS2015'
        layer = 20 # county subdivisions
        filter = { 'state':'31', 'county':'071' } # Garfield county, NE
        tq = censusbuddy.TigerREST(service)
        df = tq.query(layer, filter)
        self.assertEqual(df.shape, (10,20))
        self.assertEqual(sorted(df.columns.values), sorted([
            'AREALAND', 'AREAWATER', 'BASENAME', 'CENTLAT', 'CENTLON',
            'COUNTY', 'COUSUB', 'COUSUBCC', 'COUSUBNS', 'FUNCSTAT', 'GEOID',
            'INTPTLAT', 'INTPTLON', 'LSADC', 'MTFCC', 'NAME', 'OBJECTID',
            'OID', 'STATE', 'geometry'
            ]))
        self.assertEqual(sorted(df.BASENAME.values), sorted([
            'Dry Cedar', 'Rockford', 'Willow Springs', 'Burwell', 'Bryan',
            'Kinkaid', 'Midvale', 'Roosevelt', 'Highland', 'Erina'
            ]))

        # map projection
        self.assertEqual(df.crs['init'], 'epsg:4326')

if __name__ == '__main__':
    unittest.main(verbosity=2)

