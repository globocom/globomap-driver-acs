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
import datetime
import hashlib
import logging
import re
import time

from cloudstack import CloudStackClient
from cloudstack import CloudstackService
from load import CloudstackDataLoader
from dateutil.parser import parse
from pika.exceptions import ConnectionClosed
from rabbitmq import RabbitMQClient
from settings import get_setting

from globomap_driver_acs.csv_reader import CsvReader


class Cloudstack(object):

    log = logging.getLogger(__name__)

    VM_CREATE_EVENT = 'VM.CREATE'
    VM_UPGRADE_EVENT = 'VM.UPGRADE'
    VM_DELETE_EVENT = 'VM.DESTROY'
    PATCH_ACTION = 'PATCH'
    CREATE_ACTION = 'CREATE'
    UPDATE_ACTION = 'UPDATE'
    DELETE_ACTION = 'DELETE'
    KEY_TEMPLATE = 'globomap_%s'

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
            'VM-DESTROY.com-cloud-vm-VirtualMachine.*'
        ])

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
                    updates = self._create_updates(raw_msg)
                    for update in updates:
                        callback(update)

                    self.rabbitmq.ack_message(delivery_tag)
                else:
                    return
            except ConnectionClosed:
                self.log.error('Error connecting to RabbitMQ, reconnecting')
                self._connect_rabbit()
            except:
                self.rabbitmq.nack_message(delivery_tag)
                raise

    def full_load(self):
        CloudstackDataLoader(self.env).run()

    def _create_updates(self, raw_msg):
        """
        Creates update documents for every create, upgrade or power
        state change events for Cloudstack virtual machines. On newly created
        virtual machines also creates edges documents so the VM can be
        linked to it's client business service and business process
        """
        updates = []

        if self.is_vm_update_event(raw_msg):
            cloudstack_service = self._get_cloudstack_service()
            vm_id = self._get_vm_id(raw_msg)
            vm = cloudstack_service.get_virtual_machine(vm_id)

            if vm:
                self.log.debug('Creating updates for event: %s' % raw_msg)
                project = cloudstack_service.get_project(vm.get('projectid'))
                if not project:
                    project = dict()
                self._create_vm_updates(updates, raw_msg, project, vm)

        elif self._is_vm_delete_event(raw_msg):
            self.log.debug('Creating cleanup updates for event: %s' % raw_msg)
            self._create_vm_cleanup_updates(updates, raw_msg)

        return updates

    def _create_vm_updates(self, updates, raw_msg, project, vm):
        hostname = vm.get('hostname')
        comp_unit = self._format_comp_unit_document(
            project, vm, raw_msg['eventDateTime']
        )
        vm_update_document = self._create_update_document(
            self.PATCH_ACTION, 'comp_unit', 'collections',
            comp_unit, self.KEY_TEMPLATE % comp_unit['id']
        )
        updates.append(vm_update_document)
        self._create_host_update(updates, comp_unit, hostname)

        if self.project_allocations and self._is_vm_create_event(raw_msg):
            self._create_process_update(
                updates, comp_unit)
            self._create_client_update(
                updates, project.get('name'), comp_unit)
            self._create_business_service_update(
                updates, project.get('name'), comp_unit)

    def _format_comp_unit_document(self, project, vm, event_date=None):
        return {
            'id': vm['id'],
            'name': vm['name'],
            'timestamp': self._parse_date(event_date),
            'provider': 'globomap',
            'properties': {
                'uuid': vm.get('id', ''),
                'state': vm.get('state', ''),
                'host': vm.get('hostname', ''),
                'zone': vm.get('zonename', ''),
                'service_offering': vm.get('serviceofferingname', ''),
                'cpu_cores': vm.get('cpunumber', ''),
                'cpu_speed': vm.get('cpuspeed', ''),
                'memory': vm.get('memory', ''),
                'template': vm.get('templatename', ''),
                'project': project.get('name'),
                'account': project.get('account', vm.get('account')),
                'environment': self.env,
                'creation_date': self._parse_date(vm['created']),
            },
            'properties_metadata': {
                'uuid': {'description': 'UUID'},
                'state': {'description': 'Power state'},
                'host': {'description': 'Host name'},
                'zone': {'description': 'Zone name'},
                'service_offering': {'description': 'Compute Offering'},
                'cpu_cores': {'description': 'Number of CPU cores'},
                'cpu_speed': {'description': 'CPU speed'},
                'memory': {'description': 'RAM size'},
                'template': {'description': 'Template name'},
                'project': {'description': 'Project'},
                'account': {'description': 'Account'},
                'environment': {'description': 'Cloudstack Region'},
                'creation_date': {'description': 'Creation Date'}
            }
        }

    def _create_vm_cleanup_updates(self, updates, raw_msg):
        comp_unit_id = self._get_vm_id(raw_msg)
        key = self.KEY_TEMPLATE % comp_unit_id
        host_link_delete = self._create_delete_document(
            'host_comp_unit', 'edges', key
        )
        process_link_delete = self._create_delete_document(
            'business_process_comp_unit', 'edges', key
        )
        service_link_delete = self._create_delete_document(
            'business_service_comp_unit', 'edges', key
        )
        client_link_delete = self._create_delete_document(
            'client_comp_unit', 'edges', key
        )
        updates.append(host_link_delete)
        updates.append(process_link_delete)
        updates.append(service_link_delete)
        updates.append(client_link_delete)

    def _get_vm_id(self, msg):
        if msg.get('event') == self.VM_UPGRADE_EVENT:
            return msg.get('entityuuid')
        else:
            return msg.get('id')

    def _create_process_update(self, updates, comp_unit):
        process = 'Processamento de Dados em Modelo Virtual'

        edge = self._create_edge(
            comp_unit['id'],
            'business_process_comp_unit',
            'business_process/cmdb_{}'.format(self.hash(process)),
            'comp_unit/globomap_{}'.format(comp_unit['id'])
        )
        updates.append(edge)

    def _create_business_service_update(self, updates, prj_name, comp_unit):
        """
        Creates the edge document linking a virtual machine document to a
        business service. If the business service name is surrounded by
        <> (lesser and greater sign), like <Business>, it means that this
        is a internal business service and therefore not inserted in the
        GloboMap API, so it must be created.
        """

        allocation = self.project_allocations.get(prj_name)
        if allocation:
            business_service = allocation['business_service']
            business_service_md5 = self.hash(business_service)
            link = 'business_service/cmdb_{}'.format(business_service_md5)

            if re.search('<.+>', business_service):
                business_service_element = {
                    'id': business_service_md5,
                    'name': business_service,
                    'provider': 'globomap',
                    'timestamp': int(
                        time.mktime(datetime.datetime.now().timetuple())
                    )
                }

                updates.append(self._create_update_document(
                    self.PATCH_ACTION, 'business_service', 'collections',
                    business_service_element,
                    self.KEY_TEMPLATE % business_service_element['id']
                ))

                link = 'business_service/globomap_{}'\
                    .format(business_service_md5)

            updates.append(self._create_edge(
                comp_unit['id'], 'business_service_comp_unit',
                link, 'comp_unit/globomap_{}'.format(comp_unit['id'])
            ))

    def _create_client_update(self, updates, project_name, comp_unit):
        allocation = self.project_allocations.get(project_name)
        if allocation:
            name_md5 = self.hash(allocation['client'])
            edge = self._create_edge(
                comp_unit['id'],
                'client_comp_unit',
                'client/cmdb_{}'.format(name_md5),
                'comp_unit/globomap_{}'.format(comp_unit['id'])
            )
            updates.append(edge)

    def _create_host_update(self, updates, comp_unit, hostname):
        if hostname:
            edge = self._create_edge(
                comp_unit['id'],
                'host_comp_unit',
                'comp_unit/globomap_{}'.format(hostname),
                'comp_unit/globomap_{}'.format(comp_unit['id'])
            )
            updates.append(edge)
        else:
            delete_edge = self._create_update_document(
                self.DELETE_ACTION, 'host_comp_unit', 'edges', {},
                self.KEY_TEMPLATE % comp_unit['id']
            )
            updates.append(delete_edge)

    def _create_edge(self, id, collection, from_key, to_key):
        timestamp = int(time.mktime(datetime.datetime.now().timetuple()))
        edge = {
            'id': id,
            'provider': 'globomap',
            'timestamp': timestamp,
            'from': from_key,
            'to': to_key
        }
        return self._create_update_document(
            self.UPDATE_ACTION, collection, 'edges', edge,
            self.KEY_TEMPLATE % id
        )

    def _create_update_document(self, action, collection,
                                type, element, key=None):
        update = {
            'action': action,
            'collection': collection,
            'type': type,
            'element': element
        }
        if key:
            update['key'] = key
        return update

    def _create_delete_document(self, collection, type, key):
        return {
            'action': self.DELETE_ACTION,
            'collection': collection,
            'type': type,
            'element': {},
            'key': key
        }

    def _read_project_allocation_file(self, file_path):
        """
        Reads in a given path a CSV file describing which business
        service and client are associated with each of the cloudstack
        projects. The format of the file is described below:
        <account>,<project>,<account_project>,<client>,<business_service>,
        The second, fourth and fifth fields are considered by this code
        """
        lines = CsvReader(file_path, ',').get_lines()
        project_allocations = dict()

        for line in lines:
            project_name = line[1]
            business_service_name = line[4]
            client = line[3]
            if business_service_name and client:
                project_allocations[project_name] = {
                    'business_service': business_service_name, 'client': client
                }
        return project_allocations

    def is_vm_update_event(self, raw_msg):
        is_create = self._is_vm_create_event(raw_msg)
        is_upgrade = self._is_vm_upgrade_event(raw_msg)
        is_power_state_change = self._is_vm_power_state_event(raw_msg)
        return is_create or is_power_state_change or is_upgrade

    def _is_vm_create_event(self, msg):
        is_create_event = msg.get('event') == self.VM_CREATE_EVENT
        is_vm_resource = msg.get('resource') == 'com.cloud.vm.VirtualMachine'
        return is_create_event and is_vm_resource

    def _is_vm_delete_event(self, msg):
        is_create_event = msg.get('event') == self.VM_DELETE_EVENT
        is_vm_resource = msg.get('resource') == 'com.cloud.vm.VirtualMachine'
        return is_create_event and is_vm_resource

    def _is_vm_upgrade_event(self, msg):
        is_vm_upgrade_event = msg.get('event') == self.VM_UPGRADE_EVENT
        is_event_complete = msg.get('status') == 'Completed'
        return is_vm_upgrade_event and is_event_complete

    def _is_vm_power_state_event(self, msg):
        is_vm_upgrade_event = msg.get('resource') == 'VirtualMachine'
        is_event_complete = msg.get('status') == 'postStateTransitionEvent'
        return is_vm_upgrade_event and is_event_complete

    def _parse_date(self, event_time):
        if not event_time:
            timetuple = datetime.datetime.now().timetuple()
        else:
            timetuple = parse(event_time).replace(tzinfo=None).timetuple()
        return int(time.mktime(timetuple))

    def _get_cloudstack_service(self):
        acs_client = CloudStackClient(
            self._get_setting('API_URL'),
            self._get_setting('API_KEY'),
            self._get_setting('API_SECRET_KEY'), True
        )
        return CloudstackService(acs_client)

    def _get_setting(self, key, default=None):
        return get_setting(self.env, key, default)

    def hash(self, value):
        return hashlib.md5(value.lower()).hexdigest()
