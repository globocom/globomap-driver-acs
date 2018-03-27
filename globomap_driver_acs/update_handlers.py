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
import time

from dateutil.parser import parse

from globomap_driver_acs import settings
from globomap_driver_acs.settings import get_setting


class GloboMapUpdateHandler(object):

    KEY_TEMPLATE = 'globomap_%s'
    IAAS_PROVIDER = 'cloudstack'
    GLOBOMAP_PROVIDER = 'globomap'
    CUSTEIO_PROVIDER = 'custeio'

    def __init__(self, env, cloudstack_service):
        self.env = env
        self.cloudstack_service = cloudstack_service

    def create_document(self, action, collection, type, element, key=None):
        document = {
            'action': action,
            'collection': collection,
            'type': type,
            'element': element
        }
        if key:
            document['key'] = key
        return document

    def create_edge(self, id, collection, from_key, to_key):
        edge_document = {
            'id': id,
            'provider': self.GLOBOMAP_PROVIDER,
            'timestamp': self.now_timestamp(),
            'from': from_key,
            'to': to_key,
            'properties': {
                'environment': self.env,
                'iaas_provider': self.IAAS_PROVIDER
            },
            'properties_metadata': {
                'environment': {'description': 'Cloudstack Region'},
                'iaas_provider': {'description': 'Iaas provider name'}
            }
        }
        return self.create_document(
            GloboMapActions.UPDATE,
            collection,
            Edge.type_name(),
            edge_document,
            self.create_key(id)
        )

    def _create_delete_document(self, collection, type, key):
        return self.create_document(
            GloboMapActions.DELETE, collection, type, {}, key
        )

    def link(self, collection, value, provider=None):
        provider = provider if provider else self.GLOBOMAP_PROVIDER
        return '{}/{}_{}'.format(collection, provider, value)

    def create_key(self, value):
        return self.KEY_TEMPLATE % value

    def now_timestamp(self):
        return int(time.mktime(datetime.datetime.now().timetuple()))

    def _get_setting(self, key, default=None):
        return get_setting(self.env, key, default)


class VirtualMachineUpdateHandler(GloboMapUpdateHandler):

    def __init__(self, env, cloudstack_service):
        super(VirtualMachineUpdateHandler, self).__init__(
            env, cloudstack_service
        )

    def create_vm_updates(self, updates, raw_msg, project, vm):
        hostname = vm.get('hostname')
        comp_unit_document = self._create_comp_unit_document(
            project, vm, raw_msg['eventDateTime']
        )
        updates.append(self.create_document(
            GloboMapActions.PATCH,
            Collection.COMP_UNIT,
            Collection.type_name(),
            comp_unit_document,
            self.create_key(comp_unit_document['id'])
        ))

        if hostname:
            # Creates link between VM and Host
            HostUpdateHandler(
                self.env, self.cloudstack_service
            ).create_host_update(updates, comp_unit_document, hostname)

            # Creates link between Host and Cloudstack Zone
            ZoneUpdateHandler(
                self.env, self.cloudstack_service
            ).create_zone_update(updates, comp_unit_document, hostname)

        # Creates link between VM and Dictionary entities
        is_vm_create_event = EventTypeHandler.is_vm_create_event(raw_msg)
        if is_vm_create_event:
            DictionaryEntitiesUpdateHandler(
                self.env, self.cloudstack_service, project
            ).create_dictionary_updates(updates, comp_unit_document)

    def _create_comp_unit_document(self, project, vm, event_date=None):
        return {
            'id': vm['id'],
            'name': vm['name'],
            'timestamp': self._parse_date(event_date),
            'provider': self.GLOBOMAP_PROVIDER,
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
                'iaas_provider': self.IAAS_PROVIDER,
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
                'iaas_provider': {'description': 'Iaas provider name'},
                'creation_date': {'description': 'Creation Date'}
            }
        }

    def create_vm_cleanup_updates(self, updates, raw_msg):
        key = self.create_key(self.get_vm_id(raw_msg))

        updates.append(self._create_delete_document(
            Edge.HOST_COMP_UNIT, Edge.type_name(), key
        ))
        updates.append(self._create_delete_document(
            Edge.PROCESS_COMP_UNIT, Edge.type_name(), key
        ))
        updates.append(self._create_delete_document(
            Edge.BUSINESS_SERVICE_COMP_UNIT, Edge.type_name(), key
        ))
        updates.append(self._create_delete_document(
            Edge.CLIENT_COMP_UNIT, Edge.type_name(), key
        ))

    @staticmethod
    def get_vm_id(msg):
        if msg.get('event') == EventTypeHandler.VM_UPGRADE_EVENT:
            return msg.get('entityuuid')
        else:
            return msg.get('id')

    @staticmethod
    def _parse_date(event_time):
        if not event_time:
            timetuple = datetime.datetime.now().timetuple()
        else:
            timetuple = parse(event_time).replace(tzinfo=None).timetuple()
        return int(time.mktime(timetuple))


class HostUpdateHandler(GloboMapUpdateHandler):

    def create_host_update(self, updates, comp_unit, hostname):
        comp_unit_id = comp_unit['id']
        if hostname:
            updates.append(self.create_edge(
                comp_unit_id,
                Edge.HOST_COMP_UNIT,
                self.link(Collection.COMP_UNIT, hostname),
                self.link(Collection.COMP_UNIT, comp_unit_id)
            ))
        else:
            updates.append(self.create_document(
                GloboMapActions.DELETE,
                Edge.HOST_COMP_UNIT,
                Edge.type_name(), {},
                self.create_key(comp_unit_id)
            ))


class DictionaryEntitiesUpdateHandler(GloboMapUpdateHandler):

    def __init__(self, env, cloudstack_service, project):
        super(DictionaryEntitiesUpdateHandler, self).__init__(
            env, cloudstack_service
        )
        self.project = project

    def create_dictionary_updates(self, updates, comp_unit):
        self._create_process_update(updates, comp_unit)
        self._create_business_service_update(updates, comp_unit)
        self._create_client_update(updates, comp_unit)
        self._create_component_update(updates, comp_unit)
        self._create_sub_component_update(updates, comp_unit)
        self._create_product_update(updates, comp_unit)

    def _create_process_update(self, updates, comp_unit):
        updates.append(self.create_edge(
            comp_unit['id'],
            Edge.PROCESS_COMP_UNIT,
            self.link(Collection.PROCESS, settings.DEFAULT_PROCESS_ID,
                      self.CUSTEIO_PROVIDER),
            self.link(Collection.COMP_UNIT, comp_unit['id'])
        ))

    def _create_business_service_update(self, updates, comp_unit):
        if self.project and self.project.get('businessserviceid'):
            business_service_id = self.project['businessserviceid']

            from_link = self.link(
                Collection.BUSINESS_SERVICE,
                business_service_id,
                self.CUSTEIO_PROVIDER
            )

            updates.append(self.create_edge(
                comp_unit['id'],
                Edge.BUSINESS_SERVICE_COMP_UNIT,
                from_link,
                self.link(Collection.COMP_UNIT, comp_unit['id'])
            ))

    def _create_client_update(self, updates, comp_unit):
        if self.project and self.project.get('clientid'):
            client_id = self.project['clientid']

            updates.append(self.create_edge(
                comp_unit['id'],
                Edge.CLIENT_COMP_UNIT,
                self.link(Collection.CLIENT, client_id,
                          self.CUSTEIO_PROVIDER),
                self.link(Collection.COMP_UNIT, comp_unit['id']),
            ))

    def _create_component_update(self, updates, comp_unit):
        if self.project and self.project.get('componentid'):
            component_id = self.project['componentid']

            updates.append(self.create_edge(
                comp_unit['id'],
                Edge.COMPONENT_COMP_UNIT,
                self.link(Collection.COMPONENT, component_id,
                          self.CUSTEIO_PROVIDER),
                self.link(Collection.COMP_UNIT, comp_unit['id']),
            ))

    def _create_sub_component_update(self, updates, comp_unit):
        if self.project and self.project.get('subcomponentid'):
            sub_component_id = self.project['subcomponentid']

            updates.append(self.create_edge(
                comp_unit['id'],
                Edge.SUB_COMPONENT_COMP_UNIT,
                self.link(Collection.SUB_COMPONENT, sub_component_id,
                          self.CUSTEIO_PROVIDER),
                self.link(Collection.COMP_UNIT, comp_unit['id']),
            ))

    def _create_product_update(self, updates, comp_unit):
        if self.project and self.project.get('productid'):
            product_id = self.project['productid']

            updates.append(self.create_edge(
                comp_unit['id'],
                Edge.PRODUCT_COMP_UNIT,
                self.link(Collection.PRODUCT, product_id,
                          self.CUSTEIO_PROVIDER),
                self.link(Collection.COMP_UNIT, comp_unit['id']),
            ))


class ZoneUpdateHandler(GloboMapUpdateHandler):

    def create_zone_update(self, updates, comp_unit, hostname):
        zone_name = comp_unit['properties']['zone']
        zone = self.cloudstack_service.get_zone_by_name(zone_name)

        self._create_zone_document(updates, zone)

        updates.append(self.create_edge(
            hostname,
            Edge.ZONE_HOST,
            self.link(Collection.ZONE, zone['id']),
            self.link(Collection.COMP_UNIT, hostname)
        ))

        self._create_region_link_update(updates, zone)

        # link between zone and virtual machines
        # to be added later in the future
        # updates.append(self.create_edge(
        #     comp_unit['id'],
        #     Edge.ZONE_COMP_UNIT,
        #     self.link(Collection.ZONE, zone['id']),
        #     self.link(Collection.COMP_UNIT, comp_unit['id']),
        # ))

    def create_zone_status_update(self, updates, zone_id):
        zone = self.cloudstack_service.get_zone_by_id(zone_id)
        self._create_zone_document(updates, zone)

    def _create_zone_document(self, updates, zone):
        zone_document = {
            'id': zone['id'],
            'name': zone['name'],
            'timestamp': self.now_timestamp(),
            'provider': self.GLOBOMAP_PROVIDER,
            'properties': {
                'uuid': zone['id'],
                'state': zone['allocationstate'],
                'iaas_provider': self.IAAS_PROVIDER
            },
            'properties_metadata': {
                'uuid': {'description': 'UUID'},
                'state': {'description': 'Zone state'},
                'iaas_provider': {'description': 'IaaS provider'}
            }
        }

        updates.append(self.create_document(
            GloboMapActions.PATCH,
            Collection.ZONE,
            Collection.type_name(),
            zone_document,
            self.create_key(zone['id'])
        ))

    def _create_region_link_update(self, updates, zone):
        updates.append(self.create_edge(
            zone['id'],
            Edge.ZONE_REGION,
            self.link(Collection.ZONE, zone['id']),
            self.link(Collection.REGION, self.env)
        ))


class RegionUpdateHandler(GloboMapUpdateHandler):

    def create_region_update(self, updates):
        region_document = {
            'id': self.env,
            'name': self.env,
            'timestamp': self.now_timestamp(),
            'provider': self.GLOBOMAP_PROVIDER,
            'properties': {
                'iaas_provider': self.IAAS_PROVIDER
            },
            'properties_metadata': {
                'iaas_provider': {'description': 'IaaS provider'}
            }
        }
        updates.append(self.create_document(
            GloboMapActions.PATCH,
            Collection.REGION,
            Collection.type_name(),
            region_document,
            self.create_key(self.env)
        ))


class EventTypeHandler(object):

    VM_CREATE_EVENT = 'VM.CREATE'
    VM_UPGRADE_EVENT = 'VM.UPGRADE'
    VM_DELETE_EVENT = 'VM.DESTROY'
    ZONE_EDIT = 'ZONE.EDIT'

    @staticmethod
    def is_vm_create_event(msg):
        is_create_event = msg.get('event') == EventTypeHandler.VM_CREATE_EVENT
        is_vm_resource = msg.get('resource') == 'com.cloud.vm.VirtualMachine'
        return is_create_event and is_vm_resource

    @staticmethod
    def is_vm_update_event(raw_msg):
        is_create = EventTypeHandler.is_vm_create_event(raw_msg)
        is_upgrade = EventTypeHandler.is_vm_upgrade_event(raw_msg)
        is_state_change = EventTypeHandler.is_vm_power_state_event(raw_msg)
        return is_create or is_state_change or is_upgrade

    @staticmethod
    def is_vm_delete_event(msg):
        is_create_event = msg.get('event') == EventTypeHandler.VM_DELETE_EVENT
        is_vm_resource = msg.get('resource') == 'com.cloud.vm.VirtualMachine'
        return is_create_event and is_vm_resource

    @staticmethod
    def is_vm_upgrade_event(msg):
        is_up_event = msg.get('event') == EventTypeHandler.VM_UPGRADE_EVENT
        is_event_complete = msg.get('status') == 'Completed'
        return is_up_event and is_event_complete

    @staticmethod
    def is_vm_power_state_event(msg):
        is_vm_upgrade_event = msg.get('resource') == 'VirtualMachine'
        is_event_complete = msg.get('status') == 'postStateTransitionEvent'
        return is_vm_upgrade_event and is_event_complete

    @staticmethod
    def is_zone_change_state_event(msg):
        is_zone_edit_event = msg.get('event') == EventTypeHandler.ZONE_EDIT
        is_event_complete = msg.get('status') == 'completed'
        return is_zone_edit_event and is_event_complete


class GloboMapActions(object):

    PATCH = 'PATCH'
    CREATE = 'CREATE'
    UPDATE = 'UPDATE'
    DELETE = 'DELETE'
    CLEAR = 'CLEAR'


class Collection(object):

    COMP_UNIT = 'comp_unit'
    ZONE = 'zone'
    REGION = 'region'
    BUSINESS_SERVICE = 'custeio_business_service'
    PROCESS = 'custeio_process'
    CLIENT = 'custeio_client'
    COMPONENT = 'custeio_component'
    SUB_COMPONENT = 'custeio_sub_component'
    PRODUCT = 'custeio_product'

    @staticmethod
    def type_name():
        return 'collections'


class Edge(object):

    ZONE_REGION = 'zone_region'
    ZONE_HOST = 'zone_host'
    ZONE_COMP_UNIT = 'zone_comp_unit'
    HOST_COMP_UNIT = 'host_comp_unit'
    CLIENT_COMP_UNIT = 'custeio_client_comp_unit'
    BUSINESS_SERVICE_COMP_UNIT = 'custeio_business_service_comp_unit'
    PROCESS_COMP_UNIT = 'custeio_process_comp_unit'
    COMPONENT_COMP_UNIT = 'custeio_component_comp_unit'
    SUB_COMPONENT_COMP_UNIT = 'custeio_sub_component_comp_unit'
    PRODUCT_COMP_UNIT = 'custeio_product_comp_unit'

    @staticmethod
    def type_name():
        return 'edges'
