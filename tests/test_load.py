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
from unittest.mock import Mock
from unittest.mock import patch

from globomap_driver_acs.load import CloudstackDataLoader


class TestLoad(unittest.TestCase):

    def tearDown(self):
        patch.stopall()

    def test_vms_given_one_project_found(self):
        projects = [{'id': '1', 'name': 'project A', 'vmtotal': 1}]
        accounts = []
        vms = []
        acs_mock = self._mock_cloudstack_service(projects, accounts, vms)
        requests_mock = self._mock_requests()
        driver_mock = self._mock_driver()

        CloudstackDataLoader('ENV', driver_mock).run()

        self.assertEqual(1, acs_mock.list_projects.call_count)
        self.assertEqual(
            1, acs_mock.list_virtual_machines_by_project.call_count)
        self.assertEqual(1, requests_mock.return_value.post.call_count)

    def test_vms_given_two_projects_found(self):
        projects = [{'id': '1', 'name': 'project A', 'vmtotal': 1},
                    {'id': '2', 'name': 'project B', 'vmtotal': 1}]
        accounts = []
        vms = []
        acs_mock = self._mock_cloudstack_service(projects, accounts, vms)
        requests_mock = self._mock_requests()
        driver_mock = self._mock_driver()

        CloudstackDataLoader('ENV', driver_mock).run()

        self.assertEqual(1, acs_mock.list_projects.call_count)
        self.assertEqual(
            2, acs_mock.list_virtual_machines_by_project.call_count)
        self.assertEqual(1, requests_mock.return_value.post.call_count)

    def test_vms_given_one_vm_found(self):
        projects = [{'id': '4', 'name': 'project A', 'vmtotal': 1}]
        accounts = []
        vms = [{'id': '1'}]
        acs_mock = self._mock_cloudstack_service(projects, accounts, vms)
        requests_mock = self._mock_requests()
        driver_mock = self._mock_driver()

        CloudstackDataLoader('ENV', driver_mock).run()

        self.assertEqual(1, acs_mock.list_projects.call_count)
        acs_mock.list_virtual_machines_by_project.assert_called_once_with(
            '4', 1, 500)
        self.assertEqual(2, requests_mock.return_value.post.call_count)

    def test_vms_given_two_vms_found(self):
        projects = [{'id': '3', 'name': 'project A', 'vmtotal': 2}]
        accounts = []
        vms = [{'id': '1'}, {'id': '2'}]
        acs_mock = self._mock_cloudstack_service(projects, accounts, vms)
        requests_mock = self._mock_requests()
        driver_mock = self._mock_driver()

        CloudstackDataLoader('ENV', driver_mock).run()

        self.assertEqual(1, acs_mock.list_projects.call_count)
        acs_mock.list_virtual_machines_by_project.assert_called_once_with(
            '3', 1, 500)
        self.assertEqual(3, requests_mock.return_value.post.call_count)

    def test_load_vm_data_given_no_projects_found(self):
        projects = []
        vms = []
        accounts = []
        acs_mock = self._mock_cloudstack_service(projects, accounts, vms)
        requests_mock = self._mock_requests()
        driver_mock = self._mock_driver()

        CloudstackDataLoader('ENV', driver_mock).run()

        self.assertEqual(1, acs_mock.list_projects.call_count)
        self.assertEqual(
            0, acs_mock.list_virtual_machines_by_project.call_count)
        self.assertEqual(1, requests_mock.return_value.post.call_count)

    def test_get_clear_request(self):
        self._mock_requests()
        clear_request = CloudstackDataLoader('ENV', None)._clear(
            'comp_unit', 'collections', 10000)

        self.assertEqual('CLEAR', clear_request['action'])
        self.assertEqual('comp_unit', clear_request['collection'])
        self.assertEqual('collections', clear_request['type'])

    def _mock_cloudstack_service(self, projects, accounts, vms):
        patch('globomap_driver_acs.load.CloudStackClient').start()
        mock = patch(
            'globomap_driver_acs.load.CloudstackService'
        ).start()
        acs_service_mock = Mock()
        mock.return_value = acs_service_mock
        acs_service_mock.list_projects.return_value = projects
        acs_service_mock.list_accounts.return_value = accounts
        acs_service_mock.list_virtual_machines_by_project.return_value = vms
        return acs_service_mock

    def _mock_requests(self, status_code=202, content=None):
        patch('globomap_driver_acs.load.auth').start()
        requests_mock = patch('globomap_driver_acs.load.Update').start()

        response = Mock(
            status_code=status_code,
            content='{"jobid": "16592f98-5756-4b48-b61c-d1c16fee10df"}'
        )
        requests_mock.return_value.post = response
        return requests_mock

    def _mock_driver(self):
        def driver_mock(event):
            return [{}]
        return driver_mock
