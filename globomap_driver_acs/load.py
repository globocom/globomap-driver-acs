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
import logging
import os
import datetime
import requests

from time import time
from globomap_driver_acs.cloudstack import CloudStackClient
from globomap_driver_acs.cloudstack import CloudstackService
from globomap_driver_acs.settings import get_setting
from globomap_driver_acs.update_handlers import Collection
from globomap_driver_acs.update_handlers import Edge
from globomap_driver_acs.update_handlers import GloboMapActions


class CloudstackDataLoader(object):

    log = logging.getLogger(__name__)

    def __init__(self, env, create_updates):
        self.env = env
        self.create_updates = create_updates
        self.loader_endpoint = os.getenv('GLOBOMAP_LOADER_ENDPOINT')

    def run(self):
        acs_service = self._get_cloudstack_service()
        projects = acs_service.list_projects()
        self.log.info("%s projects found. Processing:" % len(projects))
        start_time = int(time())

        for project in projects:
            prj_name = project.get('name', project.get('displaytext'))
            self.log.info("Processing project %s" % prj_name)
            vms = acs_service.list_virtual_machines_by_project(project['id'])
            self.log.info("Creating %s VM events" % len(vms))

            for vm in vms:
                event = self._create_event(vm['id'])
                self._publish_updates(self.create_updates(event))

        self._clear_not_updated_elements(start_time)
        self.log.info("Processing finished")

    def _create_event(self, vm_id):
        event_date = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        return {
            "event": "VM.CREATE",
            "resource": "com.cloud.vm.VirtualMachine",
            "eventDateTime": event_date,
            "id": vm_id
        }

    def _publish_updates(self, updates):
        try:
            self._send(updates)
        except:
            self.log.exception("Unable to publish event. Aborting execution")
            raise

    def _clear_not_updated_elements(self, start_time):
        self.log.info("[Clear] Deleting old elements")
        clears = list()
        clears.append(self._clear(
            Collection.COMP_UNIT, Collection.type_name(), start_time
        ))
        clears.append(self._clear(
            Collection.ZONE, Collection.type_name(), start_time
        ))
        clears.append(self._clear(
            Edge.ZONE_HOST, Edge.type_name(), start_time
        ))
        clears.append(self._clear(
            Edge.ZONE_REGION, Edge.type_name(), start_time
        ))
        clears.append(self._clear(
            Edge.PROCESS_COMP_UNIT, Edge.type_name(), start_time
        ))
        clears.append(self._clear(
            Edge.BUSINESS_SERVICE_COMP_UNIT, Edge.type_name(), start_time
        ))
        clears.append(self._clear(
            Edge.CLIENT_COMP_UNIT, Edge.type_name(), start_time
        ))
        clears.append(self._clear(
            Edge.HOST_COMP_UNIT, Edge.type_name(), start_time
        ))
        self._send(clears)

    def _clear(self, collection, type, timestamp):
        self.log.info("Cleaning '%s' before %s" % (collection, timestamp))
        return {
            'action': GloboMapActions.CLEAR,
            'collection': collection,
            'type': type,
            'element': [[
                self._filter('timestamp', timestamp, '<'),
                self._filter('properties.environment', self.env, '=='),
                self._filter('properties.iaas_provider', 'cloudstack', '==')
            ]]
        }

    def _filter(self, field, value, operator):
        return {'field': field, 'value': value, 'operator': operator}

    def _send(self, data):
        response = requests.post(
            '{}/v1/updates'.format(self.loader_endpoint),
            data=json.dumps(data),
            headers={
                'Content-Type': 'application/json',
                'X-Driver-Name': 'cloudstack'
            }
        )

        if response.status_code != 202:
            self.log.error('Message not sent %s / %s' % (data, response.text))
        else:
            job_id = json.loads(response.content)['jobid']
            self.log.debug('Message was sent %s', response)
            self.log.info('Message was sent under JOB ID %s', job_id)

    def _get_cloudstack_service(self):
        acs_url = self._get_setting("API_URL")
        self.log.info("Connecting to ACS: %s" % acs_url)
        acs_client = CloudStackClient(
            acs_url,
            self._get_setting("API_KEY"),
            self._get_setting("API_SECRET_KEY"), True
        )
        return CloudstackService(acs_client)

    def _get_setting(self, key, default=None):
        return get_setting(self.env, key, default)
