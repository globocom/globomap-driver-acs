import json
import logging
from globomap_driver_acs.cloudstack import CloudStackClient, CloudstackService
from globomap_driver_acs.rabbitmq import RabbitMQClient
from globomap_driver_acs.settings import get_setting


class CloudstackDataLoader(object):

    log = logging.getLogger(__name__)

    def __init__(self, env):
        self.env = env

        self.rabbitmq = RabbitMQClient(
            host=self._get_setting("RMQ_HOST"),
            port=int(self._get_setting("RMQ_PORT", 5672)),
            user=self._get_setting("RMQ_USER"),
            password=self._get_setting("RMQ_PASSWORD"),
            vhost=self._get_setting("RMQ_VIRTUAL_HOST"),
            queue_name=self._get_setting("RMQ_QUEUE")
        )

    def run(self):
        acs_service = self._get_cloudstack_service()
        projects = acs_service.list_projects()
        self.log.info("%s projects found. Processing:" % len(projects))

        for project in projects:
            self.log.info("Processing project %s" % project['name'])
            vms = acs_service.list_virtual_machines_by_project(project['id'])
            self.log.info("Creating %s VM events" % len(vms))

            for vm in vms:
                event = self._create_event_object(vm['id'])
                self._publish_event(event)

        self.log.info("Processing finished")

    def _create_event_object(self, vm_id):
        return {
            "event": "VM.CREATE",
            "status": "Completed",
            "entityuuid": vm_id
        }

    def _publish_event(self, event):
        try:
            exchange = self._get_setting("RMQ_LOADER_EXCHANGE")
            key = "management-server.ActionEvent." \
                  "VM-CREATE.VirtualMachine.%s" % event['entityuuid']

            publish_ok = self.rabbitmq.post_message(
                exchange, key, json.dumps(event)
            )

            if not publish_ok:
                raise Exception("Failed to publish")
        except:
            self.log.error("Unable to publish event %s" % event)

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
