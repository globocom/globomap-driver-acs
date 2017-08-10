import unittest
from mock import patch
from globomap_driver_acs.settings import get_setting


class TestSettings(unittest.TestCase):

    def tearDown(self):
        patch.stopall()

    def test_get_setting(self):
        getenv_mock = self._mock_os_get_env('test')
        self.assertEqual('test', get_setting('ENV', 'RABBIT_MQ_HOST'))
        getenv_mock.assert_called_once_with('ACS_ENV_RABBIT_MQ_HOST')

    def test_get_setting_given_empty_value(self):
        getenv_mock = self._mock_os_get_env(None)
        self.assertEqual(None, get_setting('ENV', 'RABBIT_MQ_HOST'))
        getenv_mock.assert_called_once_with('ACS_ENV_RABBIT_MQ_HOST')

    def test_get_setting_with_default_value(self):
        getenv_mock = self._mock_os_get_env(None)
        self.assertEqual(1, get_setting('ENV', 'RABBIT_MQ_HOST', 1))
        getenv_mock.assert_called_once_with('ACS_ENV_RABBIT_MQ_HOST')

    def _mock_os_get_env(self, return_value):
        getenv_mock = patch('globomap_driver_acs.settings.os.getenv').start()
        getenv_mock._mock_return_value = return_value
        return getenv_mock
