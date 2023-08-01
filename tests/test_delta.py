import unittest
import delta_generator
import pandas as pd
from pandas.testing import assert_frame_equal


class WhenComparingTwoDataframes(unittest.TestCase):
    def setUp(self):
        df1 = pd.DataFrame(
            {
                'host_name': ['t', 'Jaguar', 'MG', 'MINI', 'Rover', 'test'],
                'registration': ['t', '1234', 'BC99BCDF', 'CD00CDE', 'DE01DEF', 'DE01DEFa'],
                'registration2': ['t', '1234', 'BC99BCDFa', 'CD00CDE', 'DE01DEF', 'DE01DEF'],
                'removed':['','','','','',''],
            }
        )

        df2 = pd.DataFrame(
            {
                'host_name': ['t', 'Jaguar', 'MG', 'MINI', 'Lotus', 'test'],
                'registration': ['t', 'AB98ABCD', 'esadf', 'CD00CDE', 'EF02EFG', 'DE01DEF'],
                'registration2': ['t', 'AB98ABCD', 'esadf', 'CD00CDE', 'EF02EFG', 'DE01DEFa'],
                'added':['','','','','',''],
            }
        )

        (self.columns_added, self.columns_removed, self.delta) = delta_generator.calculate_delta(df1, df2, 'host_name')

    def test_that_deltaframe_is_generated_correctly(self):
        expected_dataframe = pd.DataFrame({
            'host_name': ['Jaguar', 'MG', 'Rover', 'test', 'Lotus'],
            'registration': ['1234 --> AB98ABCD', 'BC99BCDF --> esadf', 'DE01DEF --> nan', 'DE01DEFa --> DE01DEF', 'nan --> EF02EFG'],        
            'registration2': ['1234 --> AB98ABCD', 'BC99BCDFa --> esadf', 'DE01DEF --> nan', 'DE01DEF --> DE01DEFa', 'nan --> EF02EFG'],      
            'change': ['changed', 'changed', 'removed', 'changed', 'added']
        })
        self.delta = self.delta.reset_index(drop=True)
        expected_dataframe= expected_dataframe.reset_index(drop=True)
        assert_frame_equal(expected_dataframe, self.delta, check_dtype=False)

    def test_that_the_correct_columns_are_listed_as_added(self):
        assert self.columns_added == ['added']

    def test_that_the_correct_columns_are_listed_as_removed(self):
        assert self.columns_removed == ['removed']



if __name__ == '__main__':
    unittest.main()
