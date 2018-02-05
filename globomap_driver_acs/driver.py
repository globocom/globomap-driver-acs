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
import logging

from pika.exceptions import ConnectionClosed
from cloudstack import CloudStackClient
from cloudstack import CloudstackService
from globomap_driver_acs.csv_reader import CsvReader
from globomap_driver_acs.update_handlers import VirtualMachineUpdateHandler
from globomap_driver_acs.update_handlers import RegionUpdateHandler
from globomap_driver_acs.update_handlers import ZoneUpdateHandler
from globomap_driver_acs.update_handlers import EventTypeHandler
from load import CloudstackDataLoader
from rabbitmq import RabbitMQClient
from settings import get_setting


class Cloudstack(object):

    log = logging.getLogger(__name__)

    def __init__(self, params):
        self.env = params.get('env')

        prj_allocation_file = self._get_setting('PROJECT_ALLOCATION_FILE')
        self.project_allocations = dict()
        if prj_allocation_file:
            self.project_allocations = self._read_project_allocation_file(
                prj_allocation_file
            )
        self._connect_rabbit()
        self._create_queue_binds()

    def process_updates(self, callback):
        """
        Reads and processes messages from the Cloudstack event bus until
        there's no message left in the target queue. Only acks message if
        processed successfully by the callback.
        """
        while True:
            delivery_tag = None
            try:
                raw_msg, delivery_tag = self.rabbitmq.get_message()
                if raw_msg:
                    for update in self._create_updates(raw_msg):
                        callback(update)

                    self.rabbitmq.ack_message(delivery_tag)
                else:
                    return
            except ConnectionClosed:
                self.log.error('Error connecting to RabbitMQ, reconnecting')
                self._connect_rabbit()
            except:
                self.log.exception('Error processing message')
                self.rabbitmq.nack_message(delivery_tag)
                raise

    def full_load(self):
        CloudstackDataLoader(self.env, self._create_updates).run()

    def _create_updates(self, raw_msg):
        """
        Creates update documents for every create, upgrade or power
        state change events for Cloudstack virtual machines. On newly created
        virtual machines also creates edges documents so the VM can be
        linked to it's client business service and business process
        """
        acs_service = self._get_cloudstack_service()
        updates = []

        vm_update_handler = VirtualMachineUpdateHandler(
            self.env, acs_service, self.project_allocations
        )

        if EventTypeHandler.is_vm_update_event(raw_msg):
            vm_id = vm_update_handler.get_vm_id(raw_msg)
            vm = acs_service.get_virtual_machine(vm_id)

            if vm:
                self.log.debug('Creating updates for event: %s' % raw_msg)
                project = acs_service.get_project(vm.get('projectid'))
                vm_update_handler.create_vm_updates(
                    updates, raw_msg, project, vm
                )

                region_handler = RegionUpdateHandler(self.env, acs_service)
                region_handler.create_region_update(updates)

        elif EventTypeHandler.is_vm_delete_event(raw_msg):
            self.log.debug('Creating cleanup updates for event: %s' % raw_msg)
            vm_update_handler.create_vm_cleanup_updates(updates, raw_msg)

        elif EventTypeHandler.is_zone_change_state_event(raw_msg):
            zone_handler = ZoneUpdateHandler(self.env, acs_service)
            zone_id = raw_msg.get('entityuuid')
            zone_handler.create_zone_status_update(updates, zone_id)

        return updates

    @staticmethod
    def _read_project_allocation_file(file_url):
        """
        Reads in a given CSV file describing which business
        service and client are associated with each of the cloudstack
        projects. The format of the file is described below:
        <account>,<project>,<account_project>,<client>,<business_service>,
        The second, fourth and fifth fields are considered by this code
        """
        project_allocations = dict()

        for line in CsvReader(file_url, ',').get_lines():
            project_name = line[1]
            business_service_name = line[4]
            client = line[3]
            if business_service_name and client:
                project_allocations[project_name] = {
                    'business_service': business_service_name,
                    'client': client
                }
        return project_allocations

    def _connect_rabbit(self):
        self.rabbitmq = RabbitMQClient(
            host=self._get_setting('RMQ_HOST'),
            port=int(self._get_setting('RMQ_PORT', 5672)),
            user=self._get_setting('RMQ_USER'),
            password=self._get_setting('RMQ_PASSWORD'),
            vhost=self._get_setting('RMQ_VIRTUAL_HOST'),
            queue_name=self._get_setting('RMQ_QUEUE')
        )

    def _create_queue_binds(self):
        exchange = self._get_setting('RMQ_EXCHANGE', 'cloudstack-events')
        self.rabbitmq.bind_routing_keys(exchange, [
            'management-server.ActionEvent.'
            'VM-UPGRADE.VirtualMachine.*',

            'management-server.ResourceStateEvent.'
            'FollowAgentPowerOnReport.VirtualMachine.*',

            'management-server.ResourceStateEvent.'
            'OperationSucceeded.VirtualMachine.*',

            'management-server.UsageEvent.'
            'VM-CREATE.com-cloud-vm-VirtualMachine.*',

            'management-server.UsageEvent.'
            'VM-DESTROY.com-cloud-vm-VirtualMachine.*',

            'management-server.ActionEvent.'
            'ZONE-EDIT.DataCenter.*'
        ])

    def _get_cloudstack_service(self):
        return CloudstackService(CloudStackClient(
            self._get_setting('API_URL'),
            self._get_setting('API_KEY'),
            self._get_setting('API_SECRET_KEY')
        ))

    def _get_setting(self, key, default=None):
        return get_setting(self.env, key, default)
