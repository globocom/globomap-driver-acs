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
import json
import pika


class RabbitMQClient(object):

    def __init__(self, host, port, user, password, vhost, queue_name):
        credentials = pika.PlainCredentials(user, password)
        parameters = pika.ConnectionParameters(
            host=host, port=port,
            virtual_host=vhost, credentials=credentials
        )
        self.queue_name = queue_name
        self.connection = pika.BlockingConnection(parameters)
        self.channel = self.connection.channel()
        self.channel.confirm_delivery()

    def get_message(self):
        method_frame, _, body = self.channel.basic_get(self.queue_name)
        if body:
            return json.loads(body), method_frame.delivery_tag
        else:
            return None, None

    def ack_message(self, delivery_tag):
        self.channel.basic_ack(delivery_tag)

    def nack_message(self, delivery_tag):
        self.channel.basic_nack(delivery_tag)

    def post_message(self, exchange_name, key, message):
        return self.channel.basic_publish(
            exchange=exchange_name,
            routing_key=key,
            body=message,
            mandatory=True,
        )

    def bind_routing_keys(self, exchange, keys):
        for key in keys:
            self.channel.queue_bind(
                exchange=exchange,
                queue=self.queue_name,
                routing_key=key
            )
