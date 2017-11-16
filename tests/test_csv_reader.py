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
from globomap_driver_acs.csv_reader import CsvReader
from mock import patch, Mock


class TestCsvReader(unittest.TestCase):

    def tearDown(self):
        patch.stopall()

    def test_read_csv_file(self):
        self._mock_get_file(True, 'tests/csv/sample.csv')
        csv_mock = self._mock_csv([
            ['account_a', 'Project A', 'account_a - Project A', 'Client A', 'Service A'],
            ['account_b', 'Project B', 'account_b - Project B', 'Client B', 'Service B'],
            ['account_c', 'Project C', 'account_c - Project C', 'Client C', 'Service C']
        ])

        reader = CsvReader('/path/to/file', ',')
        lines = list(reader.get_lines())
        self.assertEqual(3, len(lines))
        self.assertEqual(1, csv_mock.call_count)

    def test_read_csv_file_given_file_not_found(self):
        self._mock_get_file(None)

        reader = CsvReader('/path/to/file', ',')
        lines = list(reader.get_lines())
        self.assertEqual(0, len(lines))

    def test_read_csv_file_given_empty_file_path(self):
        reader = CsvReader('', ',')
        lines = list(reader.get_lines())
        self.assertEqual(0, len(lines))

    def test_read_csv_file_given_file_path_is_none(self):
        reader = CsvReader(None, ',')
        lines = list(reader.get_lines())
        self.assertEqual(0, len(lines))

    def test_read_csv_file_given_error(self):
        self._mock_get_file(True, 'tests/csv/sample.csv')
        csv_mock = self._mock_csv(Exception())

        with self.assertRaises(Exception):
            CsvReader('/path/to/file', ',')
            self.assertEqual(1, csv_mock.call_count)

    def _mock_csv(self, return_value):
        csv_mock = patch('globomap_driver_acs.csv_reader.csv.reader').start()
        if list == type(return_value):
            csv_mock.return_value = iter(return_value)
        else:
            csv_mock.side_effect = return_value
        return csv_mock

    def _mock_get_file(self, file_exists, file_path=None):
        get_file_mock = patch('globomap_driver_acs.csv_reader.requests.get').start()
        if file_exists:
            get_file_mock.return_value = Mock(status_code=200, content=self._open_file(file_path))
        else:
            get_file_mock.return_value = Mock(status_code=404)

    def _open_file(self, file_path):
        with open(file_path) as data_file:
            return data_file.read()

