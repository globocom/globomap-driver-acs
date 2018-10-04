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
from unittest.mock import MagicMock
from unittest.mock import Mock
from unittest.mock import patch

from globomap_driver_acs.driver import Cloudstack
from globomap_driver_acs.update_handlers import DictionaryEntitiesUpdateHandler
from globomap_driver_acs.update_handlers import EventTypeHandler
from globomap_driver_acs.update_handlers import HostUpdateHandler
from globomap_driver_acs.update_handlers import RegionUpdateHandler
from globomap_driver_acs.update_handlers import VirtualMachineUpdateHandler
from globomap_driver_acs.update_handlers import ZoneUpdateHandler
from tests.util import open_json


class TestCloudstackDriver(unittest.TestCase):

    def setUp(self):
        self.project = {
            'id': '1',
            'name': 'Project A',
            'businessserviceid': '1',
            'clientid': '1',
            'componentid': '1',
            'subcomponentid': '1',
            'productid': '1'
        }

    def tearDown(self):
        patch.stopall()

    def test_format_comp_unit(self):
        self._mock_cloudstack_service(None, None, None)
        self._mock_rabbitmq_client()
        vm = open_json('tests/json/vm.json')['virtualmachine'][0]
        project = open_json('tests/json/project.json')['project'][0]
        comp_unit = self._create_vm_update_handler()._create_comp_unit_document(project, vm)

        self.assertIsNotNone(comp_unit)
        self.assertEqual(
            '3018bdf1-4843-43b3-bdcf-ba1beb63c930', comp_unit['id'])
        self.assertEqual('vm_name', comp_unit['name'])
        self.assertEqual('globomap', comp_unit['provider'])
        self.assertIsNotNone(comp_unit['timestamp'])
        self.assertEqual(14, len(comp_unit['properties'].keys()))

        for property_key in comp_unit['properties'].keys():
            property_meta = comp_unit['properties_metadata'][property_key]
            self.assertIsNotNone(comp_unit['properties'][property_key])
            self.assertIsNotNone(property_meta)
            self.assertIsNotNone(property_meta['description'])

    def test_format_create_vm_update(self):
        self._mock_rabbitmq_client()
        cloudstack_mock = self._mock_cloudstack_service(
            open_json('tests/json/vm.json')['virtualmachine'][0],
            open_json('tests/json/project.json')['project'][0],
            open_json('tests/json/zone.json')['zone'][0]
        )
        update = self._create_driver()._create_updates(
            open_json('tests/json/vm_create_event.json'))[0]

        self.assertEqual('PATCH', update['action'])
        self.assertEqual('comp_unit', update['collection'])
        self.assertEqual('collections', update['type'])
        self.assertEqual(
            'globomap_3018bdf1-4843-43b3-bdcf-ba1beb63c930', update['key'])
        self.assertTrue(cloudstack_mock.get_virtual_machine.called)
        self.assertTrue(cloudstack_mock.get_project.called)

    def test_format_create_vm_delete_document(self):
        self._mock_cloudstack_service(None, None, None)
        self._mock_rabbitmq_client()
        updates = self._create_driver()._create_updates(
            open_json('tests/json/vm_destroy_event.json'))

        host_edge_delete = updates[0]
        process_edge_delete = updates[1]
        service_edge_delete = updates[2]
        client_edge_delete = updates[3]

        self.assertEqual(4, len(updates))

        self.assertEqual('DELETE', host_edge_delete['action'])
        self.assertEqual('host_comp_unit', host_edge_delete['collection'])
        self.assertEqual('edges', host_edge_delete['type'])
        self.assertEqual(
            'globomap_3018bdf1-4843-43b3-bdcf-ba1beb63c930', host_edge_delete['key'])

        self.assertEqual('DELETE', process_edge_delete['action'])
        self.assertEqual('custeio_process_comp_unit',
                         process_edge_delete['collection'])
        self.assertEqual('edges', process_edge_delete['type'])
        self.assertEqual(
            'globomap_3018bdf1-4843-43b3-bdcf-ba1beb63c930', process_edge_delete['key'])

        self.assertEqual('DELETE', service_edge_delete['action'])
        self.assertEqual('custeio_business_service_comp_unit',
                         service_edge_delete['collection'])
        self.assertEqual('edges', service_edge_delete['type'])
        self.assertEqual(
            'globomap_3018bdf1-4843-43b3-bdcf-ba1beb63c930', service_edge_delete['key'])

        self.assertEqual('DELETE', client_edge_delete['action'])
        self.assertEqual('custeio_client_comp_unit',
                         client_edge_delete['collection'])
        self.assertEqual('edges', client_edge_delete['type'])
        self.assertEqual(
            'globomap_3018bdf1-4843-43b3-bdcf-ba1beb63c930', client_edge_delete['key'])

    def test_format_upgrade_vm_size_update(self):
        self._mock_rabbitmq_client()
        cloudstack_mock = self._mock_cloudstack_service(
            open_json('tests/json/vm.json')['virtualmachine'][0],
            open_json('tests/json/project.json')['project'][0],
            open_json('tests/json/zone.json')['zone'][0]
        )
        update = self._create_driver()._create_updates(
            open_json('tests/json/vm_upgrade_event.json'))[0]

        self.assertEqual('PATCH', update['action'])
        self.assertEqual('comp_unit', update['collection'])
        self.assertEqual('collections', update['type'])
        self.assertEqual(
            'globomap_3018bdf1-4843-43b3-bdcf-ba1beb63c930', update['key'])
        self.assertTrue(cloudstack_mock.get_virtual_machine.called)
        self.assertTrue(cloudstack_mock.get_project.called)

    def test_format_vm_power_state_update(self):
        self._mock_rabbitmq_client()
        cloudstack_mock = self._mock_cloudstack_service(
            open_json('tests/json/vm.json')['virtualmachine'][0],
            open_json('tests/json/project.json')['project'][0],
            open_json('tests/json/zone.json')['zone'][0]
        )
        update = self._create_driver()._create_updates(
            open_json('tests/json/vm_power_state_event.json'))[0]

        self.assertEqual('PATCH', update['action'])
        self.assertEqual('comp_unit', update['collection'])
        self.assertEqual('collections', update['type'])
        self.assertEqual(
            'globomap_3018bdf1-4843-43b3-bdcf-ba1beb63c930', update['key'])
        self.assertTrue(cloudstack_mock.get_virtual_machine.called)
        self.assertTrue(cloudstack_mock.get_project.called)

    def test_format_invalid_vm_power_state_update(self):
        self._mock_rabbitmq_client()
        cloudstack_mock = self._mock_cloudstack_service(
            open_json('tests/json/vm.json')['virtualmachine'][0],
            open_json('tests/json/project.json')['project'][0],
            open_json('tests/json/zone.json')['zone'][0]
        )
        update = self._create_driver()._create_updates({
            'status': 'preStateTransitionEvent'
        })

        self.assertEqual([], update)
        self.assertFalse(cloudstack_mock.get_virtual_machine.called)
        self.assertFalse(cloudstack_mock.get_project.called)

    def test_format_incomplete_upgrade_vm_size_update(self):
        self._mock_rabbitmq_client()
        cloudstack_mock = self._mock_cloudstack_service(
            open_json('tests/json/vm.json')['virtualmachine'][0],
            open_json('tests/json/project.json')['project'][0],
            open_json('tests/json/zone.json')['zone'][0]
        )
        update = self._create_driver()._create_updates({
            'status': 'Started',
            'event': 'VM.UPGRADE'
        })

        self.assertEqual([], update)
        self.assertFalse(cloudstack_mock.get_virtual_machine.called)
        self.assertFalse(cloudstack_mock.get_project.called)

    def test_format_unmapped_update(self):
        self._mock_rabbitmq_client()
        cloudstack_mock = self._mock_cloudstack_service(
            open_json('tests/json/vm.json')['virtualmachine'][0],
            open_json('tests/json/project.json')['project'][0],
            open_json('tests/json/zone.json')['zone'][0]
        )
        update = self._create_driver()._create_updates({
            'status': 'Completed',
            'event': 'VM.START'
        })

        self.assertEqual([], update)
        self.assertFalse(cloudstack_mock.get_virtual_machine.called)
        self.assertFalse(cloudstack_mock.get_project.called)

    def test_format_update_given_no_vm_found(self):
        self._mock_rabbitmq_client()
        cloudstack_mock = self._mock_cloudstack_service(None, None, None)
        update = self._create_driver()._create_updates(
            open_json('tests/json/vm_create_event.json'))

        self.assertEqual([], update)
        self.assertTrue(cloudstack_mock.get_virtual_machine.called)
        self.assertFalse(cloudstack_mock.get_project.called)

    def test_format_update_wrong_event(self):
        self._mock_rabbitmq_client()
        cloudstack_mock = self._mock_cloudstack_service(None, None, None)
        update = self._create_driver()._create_updates(
            open_json('tests/json/vm_create_wrong_event.json'))

        self.assertEqual([], update)
        self.assertFalse(cloudstack_mock.get_virtual_machine.called)
        self.assertFalse(cloudstack_mock.get_project.called)

    def test_process_updates(self):
        rabbit_client_mock = self._mock_rabbitmq_client(
            open_json('tests/json/vm_create_event.json'))
        cloudstack_mock = self._mock_cloudstack_service(
            open_json('tests/json/vm.json')['virtualmachine'][0],
            open_json('tests/json/project.json')['project'][0],
            open_json('tests/json/zone.json')['zone'][0]
        )

        def callback(update):
            if update['action'] == 'PATCH' and update['collection'] == 'comp_unit':
                self.assertEqual('collections', update['type'])
                self.assertEqual(
                    'globomap_3018bdf1-4843-43b3-bdcf-ba1beb63c930', update['key'])
            elif update['action'] == 'UPDATE' and update['collection'] == 'zone':
                self.assertEqual('collections', update['type'])
                self.assertEqual(
                    'globomap_35ae56ee-273a-46da-8422-fe2b3490c76a', update['key'])
            elif update['action'] == 'UPDATE' and update['collection'] == 'host_comp_unit':
                self.assertEqual('edges', update['type'])
                self.assertEqual(
                    'globomap_3018bdf1-4843-43b3-bdcf-ba1beb63c930', update['key'])
            elif update['action'] == 'UPDATE' and update['collection'] == 'zone_host':
                self.assertEqual('edges', update['type'])
                self.assertEqual('globomap_hostname', update['key'])
            elif update['action'] == 'UPDATE' and update['collection'] == 'zone_region':
                self.assertEqual('edges', update['type'])
                self.assertEqual(
                    'globomap_35ae56ee-273a-46da-8422-fe2b3490c76a', update['key'])
            elif update['action'] == 'UPDATE' and update['collection'] == 'region':
                self.assertEqual('collections', update['type'])
                self.assertEqual('globomap_ENV', update['key'])
            elif update['action'] == 'UPDATE' and update['collection'] == 'custeio_process_comp_unit':
                self.assertEqual('edges', update['type'])
                self.assertEqual(
                    'globomap_3018bdf1-4843-43b3-bdcf-ba1beb63c930', update['key'])
            elif update['action'] == 'UPDATE' and update['collection'] == 'custeio_business_service_comp_unit':
                self.assertEqual('edges', update['type'])
                self.assertEqual(
                    'globomap_3018bdf1-4843-43b3-bdcf-ba1beb63c930', update['key'])
            elif update['action'] == 'UPDATE' and update['collection'] == 'custeio_client_comp_unit':
                self.assertEqual('edges', update['type'])
                self.assertEqual(
                    'globomap_3018bdf1-4843-43b3-bdcf-ba1beb63c930', update['key'])
            elif update['action'] == 'UPDATE' and update['collection'] == 'custeio_component_comp_unit':
                self.assertEqual('edges', update['type'])
                self.assertEqual(
                    'globomap_3018bdf1-4843-43b3-bdcf-ba1beb63c930', update['key'])
            elif update['action'] == 'UPDATE' and update['collection'] == 'custeio_sub_component_comp_unit':
                self.assertEqual('edges', update['type'])
                self.assertEqual(
                    'globomap_3018bdf1-4843-43b3-bdcf-ba1beb63c930', update['key'])
            elif update['action'] == 'UPDATE' and update['collection'] == 'custeio_product_comp_unit':
                self.assertEqual('edges', update['type'])
                self.assertEqual(
                    'globomap_3018bdf1-4843-43b3-bdcf-ba1beb63c930', update['key'])
            else:
                self.fail()

        self._create_driver().process_updates(callback)

        self.assertTrue(cloudstack_mock.get_virtual_machine.called)
        self.assertTrue(cloudstack_mock.get_project.called)
        self.assertEqual(1, rabbit_client_mock.ack_message.call_count)
        self.assertEqual(0, rabbit_client_mock.nack_message.call_count)

    def test_process_updates_given_vm_without_project(self):
        rabbit_client_mock = self._mock_rabbitmq_client(
            open_json('tests/json/vm_create_event.json'))
        cloudstack_mock = self._mock_cloudstack_service(
            open_json('tests/json/vm.json')['virtualmachine'][0],
            dict(),
            open_json('tests/json/zone.json')['zone'][0]
        )

        def callback(update):
            if update['action'] == 'PATCH':
                self.assertIsNone(
                    update['element']['properties'].get('project'))

        self._create_driver().process_updates(callback)

        self.assertTrue(cloudstack_mock.get_virtual_machine.called)
        self.assertTrue(cloudstack_mock.get_project.called)
        self.assertEqual(1, rabbit_client_mock.ack_message.call_count)
        self.assertEqual(0, rabbit_client_mock.nack_message.call_count)

    def test_get_updates_given_exception(self):
        rabbit_client_mock = self._mock_rabbitmq_client(
            open_json('tests/json/vm_create_event.json'))
        cloudstack_mock = self._mock_cloudstack_service(
            open_json('tests/json/vm.json')['virtualmachine'][0],
            open_json('tests/json/project.json')['project'][0],
            open_json('tests/json/zone.json')['zone'][0]
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
        self._mock_cloudstack_service(None, None, None)

        def callback(update):
            self.fail()
        self._create_driver().process_updates(callback)

    def test_parse_date(self):
        self._mock_rabbitmq_client()
        self.assertEqual(946692000, self._create_vm_update_handler(
        )._parse_date('2000-01-01 00:00:00 -0300'))
        self.assertEqual(946692000, self._create_vm_update_handler(
        )._parse_date('2000-01-01T00:00:00-0300'))
        self.assertIsNotNone(
            self._create_vm_update_handler()._parse_date(None))

    def test_is_vm_create_event(self):
        self.assertTrue(EventTypeHandler.is_vm_create_event({
            'resource': 'com.cloud.vm.VirtualMachine',
            'event': 'VM.CREATE'
        }))

        self.assertFalse(EventTypeHandler.is_vm_create_event({
            'status': 'Completed',
            'event': 'VM.CREATE'
        }))

        self.assertFalse(EventTypeHandler.is_vm_create_event({
            'event': 'VM.START'
        }))

        self.assertFalse(EventTypeHandler.is_vm_create_event({}))

    def test_is_vm_delete_event(self):

        self.assertTrue(EventTypeHandler.is_vm_delete_event({
            'resource': 'com.cloud.vm.VirtualMachine',
            'event': 'VM.DESTROY'
        }))

        self.assertFalse(EventTypeHandler.is_vm_create_event({
            'status': 'Completed',
            'event': 'VM.DESTROY'
        }))

        self.assertFalse(EventTypeHandler.is_vm_create_event({
            'event': 'VM.CREATE'
        }))

        self.assertFalse(EventTypeHandler.is_vm_create_event({}))

    def test_is_vm_upgrade_event(self):
        self.assertTrue(EventTypeHandler.is_vm_upgrade_event({
            'status': 'Completed',
            'event': 'VM.UPGRADE'
        }))

        self.assertFalse(EventTypeHandler.is_vm_upgrade_event({
            'event': 'VM.UPGRADE'
        }))

        self.assertFalse(EventTypeHandler.is_vm_upgrade_event({}))

    def test_is_vm_power_state_event(self):
        self.assertTrue(EventTypeHandler.is_vm_power_state_event({
            'status': 'postStateTransitionEvent',
            'resource': 'VirtualMachine'
        }))

        self.assertFalse(EventTypeHandler.is_vm_power_state_event({
            'status': 'pretStateTransitionEvent',
            'resource': 'VirtualMachine'
        }))

        self.assertFalse(EventTypeHandler.is_vm_power_state_event({
            'resource': 'VirtualMachine'
        }))

        self.assertFalse(EventTypeHandler.is_vm_power_state_event({}))

    def test_create_process_update(self):
        self._mock_rabbitmq_client()
        handler = self._create_dictionary_update_handler()
        updates = []
        handler._create_process_update(updates, {'id': '123'})
        self.assertEqual(1, len(updates))
        element = updates[0]['element']

        self.assertEqual('UPDATE', updates[0]['action'])
        self.assertEqual('globomap_123', updates[0]['key'])
        self.assertEqual('custeio_process_comp_unit', updates[0]['collection'])
        self.assertEqual(
            'custeio_process/custeio_7a9456320f328700fd7f91dbe1050e27', element['from'])
        self.assertEqual('comp_unit/globomap_123', element['to'])
        self.assertEqual('globomap', element['provider'])
        self.assertEqual('123', element['id'])

    def test_create_client_update(self):
        self._mock_rabbitmq_client()
        handler = self._create_dictionary_update_handler()

        updates = []
        handler._create_client_update(updates, {'id': '123'})
        element = updates[0]['element']

        self.assertEqual(1, len(updates))
        self.assertEqual('UPDATE', updates[0]['action'])
        self.assertEqual('globomap_123', updates[0]['key'])
        self.assertEqual('custeio_client_comp_unit', updates[0]['collection'])
        self.assertEqual('custeio_client/custeio_1', element['from'])
        self.assertEqual('comp_unit/globomap_123', element['to'])
        self.assertEqual('globomap', element['provider'])
        self.assertEqual('123', element['id'])

    def test_create_client_update_given_project_not_found(self):
        self._mock_rabbitmq_client()
        handler = self._create_dictionary_update_handler()
        handler.project = None

        updates = []
        handler._create_client_update(updates, {'id': '123'})
        self.assertEqual(0, len(updates))

    def test_create_business_service_update(self):
        self._mock_rabbitmq_client()
        handler = self._create_dictionary_update_handler()

        updates = []
        handler._create_business_service_update(updates, {'id': '123'})
        element = updates[0]['element']

        self.assertEqual(1, len(updates))
        self.assertEqual('UPDATE', updates[0]['action'])
        self.assertEqual(int, type(element['timestamp']))
        self.assertEqual('globomap_123', updates[0]['key'])
        self.assertEqual('custeio_business_service_comp_unit',
                         updates[0]['collection'])
        self.assertEqual('custeio_business_service/custeio_1', element['from'])
        self.assertEqual('comp_unit/globomap_123', element['to'])
        self.assertEqual('globomap', element['provider'])
        self.assertEqual('123', element['id'])

    def test_create_business_service_update_project_not_found(self):
        self._mock_rabbitmq_client()
        handler = self._create_dictionary_update_handler()
        handler.project = None

        updates = []
        handler._create_business_service_update(updates, {'id': '123'})
        self.assertEqual(0, len(updates))

    def test_create_component_update(self):
        self._mock_rabbitmq_client()
        handler = self._create_dictionary_update_handler()

        updates = []
        handler._create_component_update(updates, {'id': '123'})
        element = updates[0]['element']

        self.assertEqual(1, len(updates))
        self.assertEqual('UPDATE', updates[0]['action'])
        self.assertEqual(int, type(element['timestamp']))
        self.assertEqual('globomap_123', updates[0]['key'])
        self.assertEqual('custeio_component_comp_unit',
                         updates[0]['collection'])
        self.assertEqual('custeio_component/custeio_1', element['from'])
        self.assertEqual('comp_unit/globomap_123', element['to'])
        self.assertEqual('globomap', element['provider'])
        self.assertEqual('123', element['id'])

    def test_create_component_update_project_not_found(self):
        self._mock_rabbitmq_client()
        handler = self._create_dictionary_update_handler()
        handler.project = None

        updates = []
        handler._create_component_update(updates, {'id': '123'})
        self.assertEqual(0, len(updates))

    def test_create_sub_component_update(self):
        self._mock_rabbitmq_client()
        handler = self._create_dictionary_update_handler()

        updates = []
        handler._create_sub_component_update(updates, {'id': '123'})
        element = updates[0]['element']

        self.assertEqual(1, len(updates))
        self.assertEqual('UPDATE', updates[0]['action'])
        self.assertEqual(int, type(element['timestamp']))
        self.assertEqual('globomap_123', updates[0]['key'])
        self.assertEqual('custeio_sub_component_comp_unit',
                         updates[0]['collection'])
        self.assertEqual('custeio_sub_component/custeio_1', element['from'])
        self.assertEqual('comp_unit/globomap_123', element['to'])
        self.assertEqual('globomap', element['provider'])
        self.assertEqual('123', element['id'])

    def test_create_sub_component_update_project_not_found(self):
        self._mock_rabbitmq_client()
        handler = self._create_dictionary_update_handler()
        handler.project = None

        updates = []
        handler._create_sub_component_update(updates, {'id': '123'})
        self.assertEqual(0, len(updates))

    def test_create_product_update(self):
        self._mock_rabbitmq_client()
        handler = self._create_dictionary_update_handler()

        updates = []
        handler._create_product_update(updates, {'id': '123'})
        element = updates[0]['element']

        self.assertEqual(1, len(updates))
        self.assertEqual('UPDATE', updates[0]['action'])
        self.assertEqual(int, type(element['timestamp']))
        self.assertEqual('globomap_123', updates[0]['key'])
        self.assertEqual('custeio_product_comp_unit', updates[0]['collection'])
        self.assertEqual('custeio_product/custeio_1', element['from'])
        self.assertEqual('comp_unit/globomap_123', element['to'])
        self.assertEqual('globomap', element['provider'])
        self.assertEqual('123', element['id'])

    def test_create_product_update_project_not_found(self):
        self._mock_rabbitmq_client()
        handler = self._create_dictionary_update_handler()
        handler.project = None

        updates = []
        handler._create_product_update(updates, {'id': '123'})
        self.assertEqual(0, len(updates))

    def test_create_host_update(self):
        self._mock_rabbitmq_client()
        handler = self._create_host_update_handler()

        updates = []
        handler.create_host_update(updates, {'id': '123'}, 'hostname')
        element = updates[0]['element']

        self.assertEqual(1, len(updates))
        self.assertEqual('UPDATE', updates[0]['action'])
        self.assertEqual('globomap_123', updates[0]['key'])
        self.assertEqual('host_comp_unit', updates[0]['collection'])
        self.assertEqual('comp_unit/globomap_hostname', element['from'])
        self.assertEqual('comp_unit/globomap_123', element['to'])
        self.assertEqual('globomap', element['provider'])
        self.assertEqual('123', element['id'])

    def test_create_host_update_given_vm_with_no_host(self):
        self._mock_rabbitmq_client()
        handler = self._create_host_update_handler()

        updates = []
        handler.create_host_update(updates, {'id': '123'}, '')

        self.assertEqual(1, len(updates))
        self.assertEqual('DELETE', updates[0]['action'])
        self.assertEqual('globomap_123', updates[0]['key'])
        self.assertEqual('host_comp_unit', updates[0]['collection'])

    def test_create_zone_update(self):
        self._mock_rabbitmq_client()
        cloudstack_mock = self._mock_cloudstack_service(
            None, None,
            open_json('tests/json/zone.json')['zone'][0]
        )

        handler = self._create_zone_update_handler(
            cloudstack_service=cloudstack_mock)
        updates = []
        handler.create_zone_update(
            updates, {'id': '123', 'properties': {'zone': 'zone_a'}}, 'hostname')

        self.assertEqual(3, len(updates))
        self.assertEqual('UPDATE', updates[0]['action'])
        self.assertEqual(
            'globomap_35ae56ee-273a-46da-8422-fe2b3490c76a', updates[0]['key'])
        self.assertEqual('zone', updates[0]['collection'])
        self.assertEqual('35ae56ee-273a-46da-8422-fe2b3490c76a',
                         updates[0]['element']['id'])
        self.assertEqual('zone_a', updates[0]['element']['name'])
        self.assertEqual('35ae56ee-273a-46da-8422-fe2b3490c76a',
                         updates[0]['element']['properties']['uuid'])
        self.assertEqual(
            'Enabled', updates[0]['element']['properties']['state'])

        edge = updates[1]['element']
        self.assertEqual('UPDATE', updates[1]['action'])
        self.assertEqual('globomap_hostname', updates[1]['key'])
        self.assertEqual('zone_host', updates[1]['collection'])
        self.assertEqual(
            'zone/globomap_35ae56ee-273a-46da-8422-fe2b3490c76a', edge['from'])
        self.assertEqual('comp_unit/globomap_hostname', edge['to'])
        self.assertEqual('globomap', edge['provider'])
        self.assertEqual('hostname', edge['id'])

        edge = updates[2]['element']
        self.assertEqual('UPDATE', updates[2]['action'])
        self.assertEqual(
            'globomap_35ae56ee-273a-46da-8422-fe2b3490c76a', updates[2]['key'])
        self.assertEqual('zone_region', updates[2]['collection'])
        self.assertEqual(
            'zone/globomap_35ae56ee-273a-46da-8422-fe2b3490c76a', edge['from'])
        self.assertEqual('region/globomap_ENV', edge['to'])
        self.assertEqual('globomap', edge['provider'])
        self.assertEqual('35ae56ee-273a-46da-8422-fe2b3490c76a', edge['id'])

    def test_create_region_update(self):
        self._mock_rabbitmq_client()
        cloudstack_mock = self._mock_cloudstack_service(
            None, None,
            open_json('tests/json/zone.json')['zone'][0]
        )

        handler = self._create_region_update_handler(
            cloudstack_service=cloudstack_mock)
        updates = []
        handler.create_region_update(updates)

        self.assertEqual(1, len(updates))
        self.assertEqual('UPDATE', updates[0]['action'])
        self.assertEqual('globomap_ENV', updates[0]['key'])
        self.assertEqual('region', updates[0]['collection'])
        self.assertEqual('ENV', updates[0]['element']['id'])

    def test_create_edge(self):
        self._mock_rabbitmq_client()
        handler = self._create_vm_update_handler()

        service = '1'
        edge = handler.create_edge(
            '7a9456320',
            'custeio_business_service_comp_unit',
            'business_service/custeio_%s' % service,
            'comp_unit/globomap_1'
        )

        element = edge['element']
        self.assertEqual('UPDATE', edge['action'])
        self.assertEqual('globomap_7a9456320', edge['key'])
        self.assertEqual('custeio_business_service_comp_unit',
                         edge['collection'])
        self.assertEqual('edges', edge['type'])
        self.assertEqual('7a9456320', element['id'])
        self.assertEqual('globomap', element['provider'])
        self.assertIsNotNone(element['timestamp'])
        self.assertEqual(int, type(element['timestamp']))
        self.assertEqual('business_service/custeio_%s' %
                         service, element['from'])
        self.assertEqual('comp_unit/globomap_1', element['to'])

    def test_create_update_document(self):
        self._mock_rabbitmq_client()
        handler = self._create_vm_update_handler()

        update = handler.create_document(
            'CREATE', 'comp_unit', 'collections', {}, 'KEY'
        )

        self.assertEqual('CREATE', update['action'])
        self.assertEqual('comp_unit', update['collection'])
        self.assertEqual('collections', update['type'])
        self.assertEqual({}, update['element'])
        self.assertEqual('KEY', update['key'])

    def _mock_rabbitmq_client(self, data=None):
        rabbit_mq_mock = patch(
            'globomap_driver_acs.driver.RabbitMQClient').start()
        rabbit = MagicMock()
        rabbit_mq_mock.return_value = rabbit
        rabbit.get_message.side_effect = [(data, 1), (None, None)]
        return rabbit

    def _mock_cloudstack_service(self, vm, project, zone):
        patch('globomap_driver_acs.driver.CloudStackClient').start()
        mock = patch(
            'globomap_driver_acs.driver.CloudstackService'
        ).start()
        acs_service_mock = Mock()
        mock.return_value = acs_service_mock
        acs_service_mock.get_virtual_machine.return_value = vm
        acs_service_mock.get_project.return_value = project
        acs_service_mock.get_zone_by_name.return_value = zone
        return acs_service_mock

    def _mock_csv_reader(self, parsed_csv_file):
        csv_reader_mock = patch('globomap_driver_acs.driver.CsvReader').start()
        read_lines_mock = Mock()
        csv_reader_mock.return_value = read_lines_mock
        read_lines_mock.get_lines.return_value = parsed_csv_file
        return csv_reader_mock

    def _create_driver(self):
        return Cloudstack({'env': 'ENV'})

    def _create_vm_update_handler(self, cloudstack_service=None):
        return VirtualMachineUpdateHandler('ENV', cloudstack_service)

    def _create_dictionary_update_handler(self, cloudstack_service=None):
        return DictionaryEntitiesUpdateHandler('ENV', cloudstack_service, self.project)

    def _create_host_update_handler(self, cloudstack_service=None):
        return HostUpdateHandler('ENV', cloudstack_service)

    def _create_zone_update_handler(self, cloudstack_service=None):
        return ZoneUpdateHandler('ENV', cloudstack_service)

    def _create_region_update_handler(self, cloudstack_service=None):
        return RegionUpdateHandler('ENV', cloudstack_service)
