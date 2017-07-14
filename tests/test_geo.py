import unittest
import pandas
import censusbuddy.geo as cbg

class CensusBuddyGeoTest(unittest.TestCase):
    def test_entity_to_census(self):
        self.assertEqual(cbg.entity_to_census('cbsa'), 'metropolitan statistical area/micropolitan statistical area')
        self.assertEqual(cbg.entity_to_census('subbarrio'), 'subminor civil division')
        self.assertRaises(KeyError, cbg.entity_to_census, 'notakey')

    def test_state_fips(self):
        self.assertEqual(cbg.state_fips('06'), '06')
        self.assertEqual(cbg.state_fips('CA'), '06')
        self.assertEqual(cbg.state_fips('calif'), '06')
        self.assertRaises(KeyError, cbg.state_fips, 'liforn')
        self.assertEqual(cbg.state_fips('u.s. virgin'), '78')

    def test_fips_state(self):
        self.assertEqual(cbg.fips_state('01'), 'AL')
        self.assertEqual(cbg.fips_state('06'), 'CA')
        self.assertEqual(cbg.fips_state('78'), 'VI')
        self.assertRaises(KeyError, cbg.fips_state, '00')

    def test_county_fips(self):
        self.assertEqual(cbg.county_fips('06', 'los ang'), '037')
        self.assertEqual(cbg.county_fips('CA', 'los ang'), '037')
        self.assertRaises(KeyError, cbg.county_fips, 'California', 'Los Angeles')

    def test_cousub_fips(self):
        self.assertEqual(cbg.cousub_fips('06', '037', 'san fern'), '92785')
        self.assertEqual(cbg.cousub_fips('CA', 37, 'san fern'), '92785')
        self.assertRaises(KeyError, cbg.cousub_fips, '06', 'not a county', 'san fern')

if __name__ == '__main__':
    unittest.main(verbosity=2)

