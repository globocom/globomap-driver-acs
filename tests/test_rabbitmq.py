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
from unittest.mock import patch

from globomap_driver_acs.rabbitmq import RabbitMQClient


class TestRabbitMQClient(unittest.TestCase):

    def tearDown(self):
        patch.stopall()

    def setUp(self):
        self.pika_mock = self._mock_pika()

    def test_get_message(self):
        msg = b'{"action": "CREATE", "type": "comp_unit", "element": {}}'

        self.pika_mock.basic_get.return_value = (MagicMock(), None, msg)
        rabbitmq = RabbitMQClient('', '', '', '', '', 'queue_name')

        message = rabbitmq.get_message()

        self.assertIsNotNone(message)
        self.pika_mock.basic_get.assert_called_once_with('queue_name')

    def test_ack_message(self):
        rabbitmq = RabbitMQClient('', '', '', '', '', 'queue_name')

        rabbitmq.ack_message(1)
        self.pika_mock.basic_ack.assert_called_once_with(1)

    def test_nack_message(self):
        rabbitmq = RabbitMQClient('', '', '', '', '', 'queue_name')

        rabbitmq.nack_message(1)
        self.pika_mock.basic_nack.assert_called_once_with(1)

    def test_bind_routing_keys(self):
        pika_mock = self._mock_pika()
        rabbitmq = RabbitMQClient(
            'localhost', 5672, 'user', 'password', '/', 'queue_name')

        rabbitmq.bind_routing_keys('cloudstack-events', ['a', 'b', 'c'])
        self.assertEqual(3, pika_mock.queue_bind.call_count)

    def _mock_pika(self):
        pika_mock = patch('globomap_driver_acs.rabbitmq.pika').start()

        pika_mock.ConnectionParameters.return_value = MagicMock()
        connection_mock = MagicMock()
        channel_mock = MagicMock()

        connection_mock.channel.return_value = channel_mock
        pika_mock.BlockingConnection.return_value = connection_mock

        return channel_mock
