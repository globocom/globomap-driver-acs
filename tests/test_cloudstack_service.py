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
from mock import Mock
from globomap_driver_acs.cloudstack import CloudstackService
from tests.util import open_json


class TestCloudstackService(unittest.TestCase):

    def test_get_virtual_machine(self):
        mock = self._mock_list_vm(open_json('tests/json/vm.json'))
        service = CloudstackService(mock)
        vm = service.get_virtual_machine('unique_id')

        self.assertIsNotNone(vm)
        self.assertTrue(mock.listVirtualMachines.called)

    def test_get_virtual_machine_given_vm_not_found(self):
        mock = self._mock_list_vm(open_json('tests/json/empty_vm.json'))
        service = CloudstackService(mock)
        vm = service.get_virtual_machine('unique_id')

        self.assertIsNone(vm)
        self.assertTrue(mock.listVirtualMachines.called)

    def test_get_project(self):
        mock = self._mock_list_projects(open_json('tests/json/project.json'))
        service = CloudstackService(mock)
        project = service.get_project('unique_id')

        self.assertIsNotNone(project)
        self.assertTrue(mock.listProjects.called)

    def test_get_project_given_project_not_found(self):
        mock = self._mock_list_projects(open_json('tests/json/empty_project.json'))
        service = CloudstackService(mock)
        project = service.get_project('unique_id')

        self.assertIsNone(project)
        self.assertTrue(mock.listProjects.called)

    def _mock_list_vm(self, vm_json):
        mock = Mock()
        mock.listVirtualMachines.return_value = vm_json
        return mock

    def _mock_list_projects(self, project_json):
        mock = Mock()
        mock.listProjects.return_value = project_json
        return mock
