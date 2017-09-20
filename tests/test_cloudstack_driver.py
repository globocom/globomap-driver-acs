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
import hashlib
import unittest
from mock import patch, Mock, MagicMock
from globomap_driver_acs.driver import Cloudstack
from tests.util import open_json


class TestCloudstackDriver(unittest.TestCase):

    def tearDown(self):
        patch.stopall()

    def test_format_comp_unit(self):
        self._mock_rabbitmq_client()
        vm = open_json('tests/json/vm.json')['virtualmachine'][0]
        project = open_json('tests/json/project.json')['project'][0]
        compt_unit = self._create_driver()._format_comp_unit_document(vm, project)

        self.assertIsNotNone(compt_unit)
        self.assertEquals("vm-9a140a96-b304-4512-8114-f33cfd6a875c", compt_unit["id"])
        self.assertEquals("vm_name", compt_unit["name"])
        self.assertEquals("globomap", compt_unit["provider"])
        self.assertIsNotNone(compt_unit["timestamp"])
        self.assertEqual(13, len(compt_unit['properties'].keys()))

        for property_key in compt_unit['properties'].keys():
            property_meta = compt_unit['properties_metadata'][property_key]
            self.assertIsNotNone(compt_unit['properties'][property_key])
            self.assertIsNotNone(property_meta)
            self.assertIsNotNone(property_meta['description'])

    def test_format_create_vm_update(self):
        self._mock_rabbitmq_client()
        cloudstack_mock = self._mock_cloudstack_service(
            open_json('tests/json/vm.json')['virtualmachine'][0],
            open_json('tests/json/project.json')['project'][0]
        )
        update = self._create_driver()._create_updates(open_json('tests/json/vm_create_event.json'))[0]

        self.assertEquals("PATCH", update["action"])
        self.assertEquals("comp_unit", update["collection"])
        self.assertEquals("collections", update["type"])
        self.assertEquals("globomap_vm-9a140a96-b304-4512-8114-f33cfd6a875c", update["key"])
        self.assertTrue(cloudstack_mock.get_virtual_machine.called)
        self.assertTrue(cloudstack_mock.get_project.called)

    def test_format_upgrade_vm_size_update(self):
        self._mock_rabbitmq_client()
        cloudstack_mock = self._mock_cloudstack_service(
            open_json('tests/json/vm.json')['virtualmachine'][0],
            open_json('tests/json/project.json')['project'][0]
        )
        update = self._create_driver()._create_updates(open_json('tests/json/vm_upgrade_event.json'))[0]

        self.assertEquals("PATCH", update["action"])
        self.assertEquals("comp_unit", update["collection"])
        self.assertEquals("collections", update["type"])
        self.assertEquals("globomap_vm-9a140a96-b304-4512-8114-f33cfd6a875c", update["key"])
        self.assertTrue(cloudstack_mock.get_virtual_machine.called)
        self.assertTrue(cloudstack_mock.get_project.called)

    def test_format_vm_power_state_update(self):
        self._mock_rabbitmq_client()
        cloudstack_mock = self._mock_cloudstack_service(
            open_json('tests/json/vm.json')['virtualmachine'][0],
            open_json('tests/json/project.json')['project'][0]
        )
        update = self._create_driver()._create_updates(open_json('tests/json/vm_power_state_event.json'))[0]

        self.assertEquals("PATCH", update["action"])
        self.assertEquals("comp_unit", update["collection"])
        self.assertEquals("collections", update["type"])
        self.assertEquals("globomap_vm-9a140a96-b304-4512-8114-f33cfd6a875c", update["key"])
        self.assertTrue(cloudstack_mock.get_virtual_machine.called)
        self.assertTrue(cloudstack_mock.get_project.called)

    def test_format_invalid_vm_power_state_update(self):
        self._mock_rabbitmq_client()
        cloudstack_mock = self._mock_cloudstack_service(
            open_json('tests/json/vm.json')['virtualmachine'][0],
            open_json('tests/json/project.json')['project'][0]
        )
        update = self._create_driver()._create_updates({
            "status": "preStateTransitionEvent"
        })

        self.assertEqual([], update)
        self.assertFalse(cloudstack_mock.get_virtual_machine.called)
        self.assertFalse(cloudstack_mock.get_project.called)

    def test_format_incomplete_upgrade_vm_size_update(self):
        self._mock_rabbitmq_client()
        cloudstack_mock = self._mock_cloudstack_service(
            open_json('tests/json/vm.json')['virtualmachine'][0],
            open_json('tests/json/project.json')['project'][0]
        )
        update = self._create_driver()._create_updates({
            "status":"Started",
            "event": "VM.UPGRADE"
        })

        self.assertEqual([], update)
        self.assertFalse(cloudstack_mock.get_virtual_machine.called)
        self.assertFalse(cloudstack_mock.get_project.called)

    def test_format_unmapped_update(self):
        self._mock_rabbitmq_client()
        cloudstack_mock = self._mock_cloudstack_service(
            open_json('tests/json/vm.json')['virtualmachine'][0],
            open_json('tests/json/project.json')['project'][0]
        )
        update = self._create_driver()._create_updates({
            "status":"Completed",
            "event": "VM.START"
        })

        self.assertEqual([], update)
        self.assertFalse(cloudstack_mock.get_virtual_machine.called)
        self.assertFalse(cloudstack_mock.get_project.called)

    def test_format_update_given_no_vm_found(self):
        self._mock_rabbitmq_client()
        cloudstack_mock = self._mock_cloudstack_service(None, None)
        update = self._create_driver()._create_updates(open_json('tests/json/vm_create_event.json'))

        self.assertEqual([], update)
        self.assertTrue(cloudstack_mock.get_virtual_machine.called)
        self.assertFalse(cloudstack_mock.get_project.called)

    def test_format_update_wrong_event(self):
        self._mock_rabbitmq_client()
        cloudstack_mock = self._mock_cloudstack_service(None, None)
        update = self._create_driver()._create_updates(open_json('tests/json/vm_create_wrong_event.json'))

        self.assertEqual([], update)
        self.assertFalse(cloudstack_mock.get_virtual_machine.called)
        self.assertFalse(cloudstack_mock.get_project.called)

    def test_process_updates(self):
        rabbit_client_mock = self._mock_rabbitmq_client(open_json('tests/json/vm_create_event.json'))
        cloudstack_mock = self._mock_cloudstack_service(
            open_json('tests/json/vm.json')['virtualmachine'][0],
            open_json('tests/json/project.json')['project'][0]
        )

        def callback(update):
            self.assertEquals("PATCH", update["action"])
            self.assertEquals("comp_unit", update["collection"])
            self.assertEquals("collections", update["type"])
            self.assertEquals("globomap_vm-9a140a96-b304-4512-8114-f33cfd6a875c", update["key"])

        self._create_driver().process_updates(callback)

        self.assertTrue(cloudstack_mock.get_virtual_machine.called)
        self.assertTrue(cloudstack_mock.get_project.called)
        self.assertEqual(1, rabbit_client_mock.ack_message.call_count)
        self.assertEqual(0, rabbit_client_mock.nack_message.call_count)

    def test_get_updates_given_exception(self):
        rabbit_client_mock = self._mock_rabbitmq_client(open_json('tests/json/vm_create_event.json'))
        cloudstack_mock = self._mock_cloudstack_service(
            open_json('tests/json/vm.json')['virtualmachine'][0],
            open_json('tests/json/project.json')['project'][0]
        )

        def callback(update):
            raise Exception()

        with self.assertRaises(Exception):
            self._create_driver().process_updates(callback)

        self.assertTrue(cloudstack_mock.get_virtual_machine.called)
        self.assertTrue(cloudstack_mock.get_project.called)
        self.assertEqual(0, rabbit_client_mock.ack_message.call_count)
        self.assertEqual(1, rabbit_client_mock.nack_message.call_count)

    def test_get_updates_no_messages_found(self):
        self._mock_rabbitmq_client(None)
        self._mock_cloudstack_service(None, None)

        def callback(update):
            self.fail()
        self._create_driver().process_updates(callback)

    def test_parse_date(self):
        self._mock_rabbitmq_client()
        self.assertEqual(946692000, self._create_driver()._parse_date('2000-01-01 00:00:00 -0300'))
        self.assertEqual(946692000, self._create_driver()._parse_date('2000-01-01T00:00:00-0300'))
        self.assertIsNotNone(self._create_driver()._parse_date(None))

    def test_is_vm_create_event(self):
        self._mock_rabbitmq_client()
        driver = self._create_driver()

        self.assertTrue(driver._is_vm_create_event({
            "resource": "com.cloud.vm.VirtualMachine",
            "event": "VM.CREATE"
        }))

        self.assertFalse(driver._is_vm_create_event({
            "status": "Completed",
            "event": "VM.CREATE"
        }))

        self.assertFalse(driver._is_vm_create_event({
            "event": "VM.START"
        }))

        self.assertFalse(driver._is_vm_create_event({}))

    def test_is_vm_upgrade_event(self):
        self._mock_rabbitmq_client()
        driver = self._create_driver()

        self.assertTrue(driver._is_vm_upgrade_event({
            "status": "Completed",
            "event": "VM.UPGRADE"
        }))

        self.assertFalse(driver._is_vm_upgrade_event({
            "event": "VM.UPGRADE"
        }))

        self.assertFalse(driver._is_vm_upgrade_event({}))

    def test_is_vm_power_state_event(self):
        self._mock_rabbitmq_client()
        driver = self._create_driver()

        self.assertTrue(driver._is_vm_power_state_event({
            "status": "postStateTransitionEvent",
            "resource": "VirtualMachine"
        }))

        self.assertFalse(driver._is_vm_power_state_event({
              "status": "pretStateTransitionEvent",
              "resource": "VirtualMachine"
        }))

        self.assertFalse(driver._is_vm_power_state_event({
              "resource": "VirtualMachine"
        }))

        self.assertFalse(driver._is_vm_power_state_event({}))

    def test_read_project_allocation_file(self):
        self._mock_rabbitmq_client()
        csv_reader_mock = self._mock_csv_reader([
            ['account_a', 'Project A', 'account_a - Project A', 'Client A', 'Service A'],
            ['account_b', 'Project B', 'account_b - Project B', 'Client B', 'Service B'],
            ['account_c', 'Project C', 'account_c - Project C', 'Client C', 'Service C']
        ])
        driver = self._create_driver()

        project_allocations = driver._read_project_allocation_file('/path/to/file')
        self.assertIsNotNone(project_allocations)

        self.assertEqual('Client A', project_allocations['Project A']['client'])
        self.assertEqual('Client B', project_allocations['Project B']['client'])
        self.assertEqual('Client C', project_allocations['Project C']['client'])
        csv_reader_mock.assert_called_with('/path/to/file', ',')

    def test_read_project_allocation_file_given_empty_service_name(self):
        self._mock_rabbitmq_client()
        csv_reader_mock = self._mock_csv_reader([
            ['account_a', 'Project A', 'account_a - Project A', 'Client A', '']
        ])
        driver = self._create_driver()

        project_allocations = driver._read_project_allocation_file('/path/to/file')
        self.assertIsNone(project_allocations.get('Project A'))
        csv_reader_mock.assert_called_with('/path/to/file', ',')

    def test_read_project_allocation_file_given_empty_client(self):
        self._mock_rabbitmq_client()
        csv_reader_mock = self._mock_csv_reader([
            ['account_a', 'Project A', 'account_a - Project A', '', 'Service A']
        ])
        driver = self._create_driver()

        project_allocations = driver._read_project_allocation_file('/path/to/file')
        self.assertIsNone(project_allocations.get('Project A'))
        csv_reader_mock.assert_called_with('/path/to/file', ',')

    def test_create_process_update(self):
        self._mock_rabbitmq_client()
        driver = self._create_driver()
        updates = []
        driver._create_process_update(updates, {'id': '123'})
        self.assertEqual(1, len(updates))

    def test_create_client_update(self):
        self._mock_rabbitmq_client()
        driver = self._create_driver()
        driver.project_allocations = {'Project A': {'client': 'Client A'}}

        updates = []
        driver._create_client_update(updates, 'Project A', {'id': '123'})
        self.assertEqual(1, len(updates))

    def test_create_client_update_given_project_not_found(self):
        self._mock_rabbitmq_client()
        driver = self._create_driver()
        driver.project_allocations = {}

        updates = []
        driver._create_client_update(updates, 'Project A', {'id': '123'})
        self.assertEqual(0, len(updates))

    def test_create_business_service_update(self):
        self._mock_rabbitmq_client()
        driver = self._create_driver()
        driver.project_allocations = {'Project A': {'business_service': 'Business Service A'}}

        updates = []
        driver._create_business_service_update(updates, 'Project A', {'id': '123'})
        self.assertEqual(1, len(updates))
        self.assertEqual(int, type(updates[0]['element']['timestamp']))

    def test_create_business_service_update_project_not_found(self):
        self._mock_rabbitmq_client()
        driver = self._create_driver()
        driver.project_allocations = {}

        updates = []
        driver._create_business_service_update(updates, 'Project A', {'id': '123'})
        self.assertEqual(0, len(updates))

    def test_create_business_service_update_given_internal_service(self):
        self._mock_rabbitmq_client()
        driver = self._create_driver()
        driver.project_allocations = {'Project A': {'business_service': '<Internal Business Service>'}}

        updates = []
        driver._create_business_service_update(updates, 'Project A', {'id': '123'})
        business_service_creation = updates[0]
        self.assertEqual(2, len(updates))
        self.assertEqual('PATCH', business_service_creation['action'])
        self.assertEqual('business_service', business_service_creation['collection'])
        self.assertEqual('collections', business_service_creation['type'])

    def test_create_edge(self):
        self._mock_rabbitmq_client()
        driver = self._create_driver()

        service = 'Business Service A'
        edge = driver._create_edge('business_service', {'id': '1'}, service)
        element = edge['element']
        business_service_hashed_name = hashlib.md5(service.lower()).hexdigest()

        self.assertEqual('UPDATE', edge['action'])
        self.assertEqual('globomap_1', edge['key'])
        self.assertEqual('business_service_comp_unit', edge['collection'])
        self.assertEqual('edges', edge['type'])
        self.assertEqual('1', element['id'])
        self.assertEqual('globomap', element['provider'])
        self.assertIsNotNone(element['timestamp'])
        self.assertEqual(int, type(element['timestamp']))
        self.assertEqual('business_service/cmdb_%s' % business_service_hashed_name, element['from'])
        self.assertEqual('comp_unit/globomap_1', element['to'])

    def test_create_update_document(self):
        self._mock_rabbitmq_client()
        driver = self._create_driver()

        update = driver._create_update_document(
            'CREATE', 'comp_unit', 'collections', {}, 'KEY'
        )

        self.assertEqual('CREATE', update['action'])
        self.assertEqual('comp_unit', update['collection'])
        self.assertEqual('collections', update['type'])
        self.assertEqual({}, update['element'])
        self.assertEqual('KEY', update['key'])

    def _mock_rabbitmq_client(self, data=None):
        rabbit_mq_mock = patch("globomap_driver_acs.driver.RabbitMQClient").start()
        rabbit = MagicMock()
        rabbit_mq_mock.return_value = rabbit
        rabbit.get_message.side_effect = [(data, 1), (None, None)]
        return rabbit

    def _mock_cloudstack_service(self, vm, project):
        patch('globomap_driver_acs.driver.CloudStackClient').start()
        mock = patch(
            'globomap_driver_acs.driver.CloudstackService'
        ).start()
        acs_service_mock = Mock()
        mock.return_value = acs_service_mock
        acs_service_mock.get_virtual_machine.return_value = vm
        acs_service_mock.get_project.return_value = project
        return acs_service_mock

    def _mock_csv_reader(self, parsed_csv_file):
        csv_reader_mock = patch("globomap_driver_acs.driver.CsvReader").start()
        read_lines_mock = Mock()
        csv_reader_mock.return_value = read_lines_mock
        read_lines_mock.get_lines.return_value = parsed_csv_file
        return csv_reader_mock

    def _create_driver(self):
        driver = Cloudstack({'env': 'ENV'})
        return driver
