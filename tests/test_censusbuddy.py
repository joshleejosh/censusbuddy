import unittest
import pandas
import censusbuddy

class CensusBuddyTest(unittest.TestCase):
    def test_strip_prefix(self):
        df = pandas.DataFrame({ 'a':[ 'XYZfo', 'XYZbar', 'XYZ' ] })
        out = censusbuddy.strip_prefix(df.a)
        self.assertEqual(list(out.values), ['fo', 'bar', ''])

if __name__ == '__main__':
    unittest.main(verbosity=2)

