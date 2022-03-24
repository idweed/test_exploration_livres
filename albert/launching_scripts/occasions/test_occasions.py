# import unittest
# from albert.launching_scripts.occasions.occasions import OccasionsData
# import pandas as pd
#
#
# class TestOccasions(unittest.TestCase):
#     occ = OccasionsData()
#
#     def test_treat_data(self):
#         self.assertEqual(self.occ.treat_data(), True)
#
#     def test_treat_data_excel_file_is_created(self):
#         excel_filepath = '/tmp/excel_file.xlsx'
#         dataframe = pd.DataFrame({'EAN': ['1234', '3456'], 'MARQUE': ['Toto', 'Titi']})
#         self.assertEqual(self.occ.create_excel_from_dataframe(excel_filepath, dataframe), excel_filepath)
#
#     def test_treat_data_excel_file_is_not_created(self):
#         excel_filepath = '/tmp1/excel_file.xlsx'
#         dataframe = pd.DataFrame({'EAN': ['1234', '3456'], 'MARQUE': ['Toto', 'Titi']})
#         with self.assertRaises(FileNotFoundError):
#             self.occ.create_excel_from_dataframe(excel_filepath, dataframe)
#
#     def test_write_dataframes_to_excel(self):
#         # dataframe1 = pd.DataFrame({'EAN': ['1234', '3456'], 'MARQUE': ['Toto', 'Titi']})
#         dataframe3 = pd.DataFrame({'EAN': ['1234', '555'], 'MARQUE1': ['Tata', 'Titi']})
#         dataframe2 = pd.DataFrame({'EAN': ['1234', '666'], 'MARQUE2': ['Tutu', 'Titi']})
#         df = {'site': 'toto.com',
#               'df_feuille_site': dataframe2,
#               'df_feuille1': dataframe3}
#         self.assertEqual(self.occ.add_data_to_excel_file(df), True)
#
#
#
# if __name__ == '__main__':
#     unittest.main()
