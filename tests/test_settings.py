"""
   Copyright 2017 Globo.com

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
"""
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
