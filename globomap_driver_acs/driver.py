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
import re
import logging
import time
import datetime
from dateutil.parser import parse
from pika.exceptions import ConnectionClosed
from cloudstack import CloudStackClient, CloudstackService
from globomap_driver_acs.csv_reader import CsvReader
from rabbitmq import RabbitMQClient
from settings import get_setting


class Cloudstack(object):

    log = logging.getLogger(__name__)

    VM_CREATE_EVENT = "VM.CREATE"
    VM_UPGRADE_EVENT = "VM.UPGRADE"
    PATCH_ACTION = "PATCH"
    CREATE_ACTION = "CREATE"
    KEY_TEMPLATE = "globomap_%s"

    def __init__(self, params):
        self.env = params.get('env')

        prj_allocation_file = self._get_setting("PROJECT_ALLOCATION_FILE")
        self.project_allocations = dict()
        if prj_allocation_file:
            self.project_allocations = self._read_project_allocation_file(
                prj_allocation_file
            )
        self._connect_rabbit()
        self._create_queue_binds()

    def _connect_rabbit(self):
        self.rabbitmq = RabbitMQClient(
            host=self._get_setting("RMQ_HOST"),
            port=int(self._get_setting("RMQ_PORT", 5672)),
            user=self._get_setting("RMQ_USER"),
            password=self._get_setting("RMQ_PASSWORD"),
            vhost=self._get_setting("RMQ_VIRTUAL_HOST"),
            queue_name=self._get_setting("RMQ_QUEUE")
        )

    def _create_queue_binds(self):
        exchange = self._get_setting("RMQ_EXCHANGE", 'cloudstack-events')
        self.rabbitmq.bind_routing_keys(exchange, [
            'management-server.ActionEvent.'
            'VM-UPGRADE.VirtualMachine.*',

            'management-server.ResourceStateEvent.'
            'FollowAgentPowerOnReport.VirtualMachine.*',

            'management-server.UsageEvent.'
            'VM-CREATE.com-cloud-vm-VirtualMachine.*'
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
                self.log.error("Error connecting to RabbitMQ, reconnecting")
                self._connect_rabbit()
            except:
                self.rabbitmq.nack_message(delivery_tag)
                raise

    def _create_updates(self, raw_msg):
        """
        Creates update documents for every create, upgrade or power
        state change events for Cloudstack virtual machines. On newly created
        virtual machines also creates edges documents so the VM can be
        linked to it's client business service and business process
        """

        updates = []
        is_create = self._is_vm_create_event(raw_msg)
        is_upgrade = self._is_vm_upgrade_event(raw_msg)
        is_power_state_change = self._is_vm_power_state_event(raw_msg)

        if is_create or is_upgrade or is_power_state_change:
            cloudstack_service = self._get_cloudstack_service()

            vm = cloudstack_service.get_virtual_machine(
                self._get_vm_id(raw_msg)
            )

            if vm:
                self.log.debug("Creating updates for event: %s" % raw_msg)

                project = cloudstack_service.get_project(vm["projectid"])

                comp_unit = self._format_comp_unit_document(
                    vm, project, raw_msg["eventDateTime"]
                )

                vm_update_document = self._create_update_document(
                    self.PATCH_ACTION, "comp_unit", "collections",
                    comp_unit, self.KEY_TEMPLATE % comp_unit['id']
                )

                updates.append(vm_update_document)
                if self.project_allocations and is_create:
                    self._create_process_update(updates, comp_unit)
                    self._create_client_update(
                        updates, project['name'], comp_unit)
                    self._create_business_service_update(
                        updates, project['name'], comp_unit)

        return updates

    def _format_comp_unit_document(self, vm, project, event_date=None):
        return {
            "id": "vm-%s" % vm["id"],
            "name": vm["name"],
            "timestamp": self._parse_date(event_date),
            "provider": "globomap",
            "properties":  {
                "uuid": vm.get("id", ""),
                "state": vm.get("state", ""),
                "host": vm.get("hostname", ""),
                "zone": vm.get("zonename", ""),
                "service_offering": vm.get("serviceofferingname", ""),
                "cpu_cores": vm.get("cpunumber", ""),
                "cpu_speed": vm.get("cpuspeed", ""),
                "memory": vm.get("memory", ""),
                "template": vm.get("templatename", ""),
                "project": vm.get("project", ""),
                "account": project['account'],
                "environment": self.env,
                "creation_date": self._parse_date(vm["created"]),
            },
            "properties_metadata": {
                "uuid": {"description": "UUID"},
                "state": {"description": "Power state"},
                "host": {"description": "Host name"},
                "zone": {"description": "Zone name"},
                "service_offering": {"description": "Compute Offering"},
                "cpu_cores": {"description": "Number of CPU cores"},
                "cpu_speed": {"description": "CPU speed"},
                "memory": {"description": "RAM size"},
                "template": {"description": "Template name"},
                "project": {"description": "Project"},
                "account": {"description": "Account"},
                "environment": {"description": "Cloudstack Region"},
                "creation_date": {"description": "Creation Date"}
            }
        }

    def _get_vm_id(self, msg):
        if msg.get("event") is self.VM_UPGRADE_EVENT:
            return msg.get("entityuuid")
        else:
            return msg.get("id")

    def _create_process_update(self, updates, comp_unit):
        process = "Processamento de Dados em Modelo Virtual"
        updates.append(self._create_edge(
            'business_process', comp_unit, process
        ))

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
            if re.search('<.+>', business_service):
                business_service_element = {
                    'id': hashlib.md5(business_service.lower()).hexdigest(),
                    'name': business_service,
                    'provider': 'globomap',
                    'timestamp': int(
                        time.mktime(datetime.datetime.now().timetuple())
                    )
                }

                updates.append(self._create_update_document(
                    self.PATCH_ACTION, "business_service", "collections",
                    business_service_element,
                    self.KEY_TEMPLATE % business_service_element['id']
                ))

            updates.append(self._create_edge(
                'business_service', comp_unit, business_service
            ))

    def _create_client_update(self, updates, project_name, comp_unit):
        allocation = self.project_allocations.get(project_name)
        if allocation:
            updates.append(self._create_edge(
                'client', comp_unit, allocation['client']
            ))

    def _create_edge(self, collection, comp_unit, edge_from):
        edge_from = hashlib.md5(edge_from.lower()).hexdigest()
        edge = {
            'id': comp_unit['id'],
            'provider': 'globomap',
            'timestamp': int(time.mktime(datetime.datetime.now().timetuple())),
            'from': '{}/cmdb_{}'.format(collection, edge_from),
            'to': 'comp_unit/globomap_{}'.format(comp_unit['id'])
        }

        collection = "{}_comp_unit".format(collection)
        return self._create_update_document(
            self.CREATE_ACTION, collection, 'edges', edge
        )

    def _create_update_document(self, action, collection,
                                type, element, key=None):
        update = {
            "action": action,
            "collection": collection,
            "type": type,
            "element": element
        }
        if key:
            update['key'] = key
        return update

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
                    "business_service": business_service_name, "client": client
                }
        return project_allocations

    def _is_vm_create_event(self, msg):
        is_create_event = msg.get("event") == self.VM_CREATE_EVENT
        is_vm_resource = msg.get("resource") == "com.cloud.vm.VirtualMachine"
        return is_create_event and is_vm_resource

    def _is_vm_upgrade_event(self, msg):
        is_vm_upgrade_event = msg.get("event") == self.VM_UPGRADE_EVENT
        is_event_complete = msg.get("status") == "Completed"
        return is_vm_upgrade_event and is_event_complete

    def _is_vm_power_state_event(self, msg):
        is_vm_upgrade_event = msg.get("resource") == "VirtualMachine"
        is_event_complete = msg.get("status") == "postStateTransitionEvent"
        return is_vm_upgrade_event and is_event_complete

    def _parse_date(self, event_time):
        if not event_time:
            timetuple = datetime.datetime.now().timetuple()
        else:
            timetuple = parse(event_time).replace(tzinfo=None).timetuple()
        return int(time.mktime(timetuple))

    def _get_cloudstack_service(self):
        acs_client = CloudStackClient(
             self._get_setting("API_URL"),
             self._get_setting("API_KEY"),
             self._get_setting("API_SECRET_KEY"), True
        )
        return CloudstackService(acs_client)

    def _get_setting(self, key, default=None):
        return get_setting(self.env, key, default)
