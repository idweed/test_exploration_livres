import unittest

import amazon_assortiment_livres.livres as livres
import json


class MyTestCase(unittest.TestCase):
    test_object = livres.LivresOneShot()

    def test_ean_to_asin(self):
        expected_asin = '1401307450'
        input_ean = '9781401307455'
        self.assertEqual(self.test_object.convert_ean_to_asin(ean=input_ean), expected_asin)

    def test_read_excel_in_chunks(self):
        number_of_lines_in_excel_file = 879227
        self.original_dataframe = self.test_object.read_excel_in_chunks()
        self.assertEqual(len(self.original_dataframe), number_of_lines_in_excel_file)

    def test_upload_data_to_frontier(self):
        self.assertEqual(b'{"newcount":1}', self.test_object.upload_data_to_frontier())

    def test_list_data_from_frontier(self):
        byte_string = b'{"fp":"dp/009951673X"}\n{"fp":"dp/0753823799"}\n{"fp":"dp/1401307450"}\n{"fp":"dp/1416575464"}\n{"fp":"dp/2841610039"}\n{"fp":"dp/2841613291"}\n{"fp":"dp/2911546652"}\n{"fp":"dp/2951714637"}\n{"fp":"dp/B008IR59K8"}\n{"fp":"dp/nan"}\n{"fp":"{/dp/B008IR59K8}"}\n{"fp":"{/dp/B008IR59K9}"}\n'
        self.assertEqual(self.test_object.list_data_from_frontier(), byte_string)

    def test_transform_data_from_frontier(self):
        normal_string = '{"fp":"dp/009951673X"}\n{"fp":"dp/0753823799"}\n{"fp":"dp/1401307450"}\n{"fp":"dp/1416575464"}\n{"fp":"dp/2841610039"}\n{"fp":"dp/2841613291"}\n{"fp":"dp/2911546652"}\n{"fp":"dp/2951714637"}\n{"fp":"dp/B008IR59K8"}\n{"fp":"dp/nan"}\n{"fp":"{/dp/B008IR59K8}"}\n{"fp":"{/dp/B008IR59K9}"}\n'.split(
            '\n')
        tab = [json.loads(elem) for elem in normal_string if elem]
        self.assertEqual(tab, self.test_object.transform_data_from_frontier())


if __name__ == '__main__':
    unittest.main()
