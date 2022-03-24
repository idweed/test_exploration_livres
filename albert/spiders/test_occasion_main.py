from unittest import TestCase
from albert.spiders.occasion_main import get_darty_ean\


class TestOccasion_main(TestCase):
    def test_get_darty_ean_does_not_exist(self):
        self.assertEqual(get_darty_ean('fierhfirufrenifure', 'fierhfirufrenifure'), None)

    def test_get_darty_ean_exists(self):
        self.assertEqual(get_darty_ean('APPLE'.lower(), 'iPhone SE 64GO BLACK'.lower()), ['0190199503922'])
