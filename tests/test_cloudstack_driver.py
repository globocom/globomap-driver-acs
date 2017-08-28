import unittest
from mock import patch, Mock
from globomap_driver_acs.driver import Cloudstack
from tests.util import open_json


class TestCloudstackDriver(unittest.TestCase):

    def tearDown(self):
        patch.stopall()

    def test_get_virtual_machine(self):
        self._mock_rabbitmq_client()
        cloudstack_mock = self._mock_cloudstack_service(
            open_json('tests/json/vm.json')['virtualmachine'][0],
            open_json('tests/json/project.json')['project'][0]
        )
        vm = self._create_driver()._get_virtual_machine_data('uuid')

        self.assertIsNotNone(vm)
        self.assertEquals("vm-9a140a96-b304-4512-8114-f33cfd6a875c", vm["id"])
        self.assertEquals("vm_name", vm["name"])
        self.assertEquals("globomap", vm["provider"])
        self.assertIsNotNone(vm["timestamp"])
        self.assertEqual(11, len(vm['properties']))

        for property in vm['properties']:
            self.assertIsNotNone(property['key'])
            self.assertIsNotNone(property['value'])
            self.assertIsNotNone(property['description'])

        self.assertTrue(cloudstack_mock.get_virtual_machine.called)
        self.assertTrue(cloudstack_mock.get_project.called)

    def test_get_virtual_machine_expected_not_found(self):
        self._mock_rabbitmq_client()
        cloudstack_mock = self._mock_cloudstack_service(None, None)
        vm = self._create_driver()._get_virtual_machine_data('uuid')

        self.assertIsNone(vm)
        self.assertTrue(cloudstack_mock.get_virtual_machine.called)
        self.assertFalse(cloudstack_mock.get_project.called)

    def test_format_update(self):
        self._mock_rabbitmq_client()
        cloudstack_mock = self._mock_cloudstack_service(
            open_json('tests/json/vm.json')['virtualmachine'][0],
            open_json('tests/json/project.json')['project'][0]
        )
        update = self._create_driver()._format_update(open_json('tests/json/vm_create_event.json'))

        self.assertIsNotNone(update)
        self.assertEquals("PATCH", update["action"])
        self.assertEquals("comp_unit", update["collection"])
        self.assertEquals("collections", update["type"])
        self.assertEquals("globomap_vm-9a140a96-b304-4512-8114-f33cfd6a875c", update["key"])
        self.assertTrue(cloudstack_mock.get_virtual_machine.called)
        self.assertTrue(cloudstack_mock.get_project.called)

    def test_format_update_given_no_vm_found(self):
        self._mock_rabbitmq_client()
        cloudstack_mock = self._mock_cloudstack_service(None, None)
        update = self._create_driver()._format_update(open_json('tests/json/vm_create_event.json'))

        self.assertIsNone(update)
        self.assertTrue(cloudstack_mock.get_virtual_machine.called)
        self.assertFalse(cloudstack_mock.get_project.called)

    def test_format_update_given_event_not_completed(self):
        self._mock_rabbitmq_client()
        cloudstack_mock = self._mock_cloudstack_service(None, None)
        update = self._create_driver()._format_update(open_json('tests/json/vm_create_scheduled_event.json'))

        self.assertIsNone(update)
        self.assertFalse(cloudstack_mock.get_virtual_machine.called)
        self.assertFalse(cloudstack_mock.get_project.called)

    def test_get_updates(self):
        self._mock_rabbitmq_client(open_json('tests/json/vm_create_event.json'))
        cloudstack_mock = self._mock_cloudstack_service(
            open_json('tests/json/vm.json')['virtualmachine'][0],
            open_json('tests/json/project.json')['project'][0]
        )
        updates = self._create_driver().updates()
        update = updates[0]

        self.assertIsNotNone(updates)
        self.assertEqual(1, len(updates))
        self.assertEquals("PATCH", update["action"])
        self.assertEquals("comp_unit", update["collection"])
        self.assertEquals("collections", update["type"])
        self.assertEquals("globomap_vm-9a140a96-b304-4512-8114-f33cfd6a875c", update["key"])
        self.assertTrue(cloudstack_mock.get_virtual_machine.called)
        self.assertTrue(cloudstack_mock.get_project.called)

    def test_get_updates_no_messages_found(self):
        self._mock_rabbitmq_client(None)
        self._mock_cloudstack_service(None, None)
        self.assertEquals([], self._create_driver().updates())

    def test_parse_date(self):
        self._mock_rabbitmq_client()
        self.assertEqual(946692000, self._create_driver()._parse_date('2000-01-01 00:00:00 -0300'))
        self.assertEqual(946692000, self._create_driver()._parse_date('2000-01-01T00:00:00-0300'))
        self.assertIsNotNone(self._create_driver()._parse_date(None))

    def _mock_rabbitmq_client(self, data=None):
        rabbit_mq_mock = patch("globomap_driver_acs.driver.RabbitMQClient").start()
        read_messages_mock = Mock()
        rabbit_mq_mock.return_value = read_messages_mock
        read_messages_mock.get_message.return_value = data
        return read_messages_mock

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

    def _create_driver(self):
        driver = Cloudstack({'env': 'ENV'})
        return driver
