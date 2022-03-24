import unittest
import pytest
from albert.launching_scripts.script_base import BaseScrapinghubScript


class ConnectToScrapinghubTestCase(unittest.TestCase):
    script = BaseScrapinghubScript(project_name='toto')

    def test_connect_to_client(self):
        with pytest.raises(Exception):
            self.script.connect_to_client(None, None, 'toto')

    def test_scrapinghub_connection(self):
        self.script.connect_to_scrapinghub()
        assert self.script.client is not None

    def test_run_shub_spider(self):
        with pytest.raises(Exception):
            self.script.run_scrapinghub_spider('to', 'to')
        assert 'invalid values for shub spider'
        with pytest.raises(Exception):
            self.script.run_scrapinghub_spider(436828, 'non_existant_project')
        assert 'Project doesnt exist'
        self.script.client.close()

    def test_get_shub_job_data(self):
        self.script.connect_to_scrapinghub()
        with pytest.raises(Exception):
            self.script.get_scrapinghub_job_data(123)
        assert 'Job data not available - invalid job_key'
        assert isinstance(self.script.get_scrapinghub_job_data('436828/3/95'), list)


if __name__ == '__main__':
    unittest.main()
