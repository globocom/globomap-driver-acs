import unittest
from mock import Mock, patch
from globomap_driver_acs.load import CloudstackDataLoader


class TestLoad(unittest.TestCase):

    def tearDown(self):
        patch.stopall()

    def test_vms_given_one_project_found(self):
        projects = [{'id': '1', 'name': 'project A'}]
        vms = []
        acs_mock = self._mock_cloudstack_service(projects, vms)
        rabbit_mock = self._mock_rabbitmq_client()

        CloudstackDataLoader('ENV').run()

        self.assertEqual(1, acs_mock.list_projects.call_count)
        self.assertEqual(1, acs_mock.list_virtual_machines_by_project.call_count)
        self.assertEqual(0, rabbit_mock.post_message.call_count)

    def test_vms_given_two_projects_found(self):
        projects = [{'id': '1', 'name': 'project A'}, {'id': '2', 'name': 'project B'}]
        vms = []
        acs_mock = self._mock_cloudstack_service(projects, vms)
        rabbit_mock = self._mock_rabbitmq_client()

        CloudstackDataLoader('ENV').run()

        self.assertEqual(1, acs_mock.list_projects.call_count)
        self.assertEqual(2, acs_mock.list_virtual_machines_by_project.call_count)
        self.assertEqual(0, rabbit_mock.post_message.call_count)

    def test_vms_given_one_vm_found(self):
        projects = [{'id': '1', 'name': 'project A'}]
        vms = [{'id': '1'}]
        acs_mock = self._mock_cloudstack_service(projects, vms)
        rabbit_mock = self._mock_rabbitmq_client()

        CloudstackDataLoader('ENV').run()

        self.assertEqual(1, acs_mock.list_projects.call_count)
        acs_mock.list_virtual_machines_by_project.assert_called_once_with('1')
        self.assertEqual(1, rabbit_mock.post_message.call_count)

    def test_vms_given_two_vms_found(self):
        projects = [{'id': '1', 'name': 'project A'}]
        vms = [{'id': '1'}, {'id': '1'}]
        acs_mock = self._mock_cloudstack_service(projects, vms)
        rabbit_mock = self._mock_rabbitmq_client()

        CloudstackDataLoader('ENV').run()

        self.assertEqual(1, acs_mock.list_projects.call_count)
        acs_mock.list_virtual_machines_by_project.assert_called_once_with('1')
        self.assertEqual(2, rabbit_mock.post_message.call_count)

    def test_load_vm_data_given_no_projects_found(self):
        projects = []
        vms = []
        acs_mock = self._mock_cloudstack_service(projects, vms)
        rabbit_mock = self._mock_rabbitmq_client()

        CloudstackDataLoader('ENV').run()

        self.assertEqual(1, acs_mock.list_projects.call_count)
        self.assertEqual(0, acs_mock.list_virtual_machines_by_project.call_count)
        self.assertEqual(0, rabbit_mock.post_message.call_count)

    def _mock_cloudstack_service(self, projects, vms):
        patch('globomap_driver_acs.load.CloudStackClient').start()
        mock = patch(
            'globomap_driver_acs.load.CloudstackService'
        ).start()
        acs_service_mock = Mock()
        mock.return_value = acs_service_mock
        acs_service_mock.list_projects.return_value = projects
        acs_service_mock.list_virtual_machines_by_project.return_value = vms
        return acs_service_mock

    def _mock_rabbitmq_client(self, data=None):
        rabbit_mq_mock = patch("globomap_driver_acs.load.RabbitMQClient").start()
        read_messages_mock = Mock()
        rabbit_mq_mock.return_value = read_messages_mock
        read_messages_mock.get_message.return_value = data
        return read_messages_mock
