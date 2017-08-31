import logging
import time
import datetime
from dateutil.parser import parse
from pika.exceptions import ConnectionClosed
from cloudstack import CloudStackClient, CloudstackService
from rabbitmq import RabbitMQClient
from settings import get_setting


class Cloudstack(object):

    log = logging.getLogger(__name__)

    def __init__(self, params):
        self.env = params.get('env')
        self._connect_rabbit()

    def _connect_rabbit(self):
        self.rabbitmq = RabbitMQClient(
            host=self._get_setting("RMQ_HOST"),
            port=int(self._get_setting("RMQ_PORT", 5672)),
            user=self._get_setting("RMQ_USER"),
            password=self._get_setting("RMQ_PASSWORD"),
            vhost=self._get_setting("RMQ_VIRTUAL_HOST"),
            queue_name=self._get_setting("RMQ_QUEUE")
        )

    def updates(self, number_messages=1):
        """Return list of updates"""
        try:
            return self._get_update(number_messages).next()
        except StopIteration:
            return []

    def _get_update(self, number_messages=1):
        messages = []
        while True:
            update = None
            message = None
            try:
                message = self.rabbitmq.get_message()
                if message:
                    update = self._format_update(message)
            except StopIteration:
                if messages:
                    yield messages
                raise StopIteration
            except ConnectionClosed:
                self._connect_rabbit()
            else:
                if update:
                    messages.append(update)
                if len(messages) == number_messages:
                    yield messages
                    messages = []

                if not message:
                    break

    def _format_update(self, msg):
        is_create_event = msg["event"] == "VM.CREATE"
        is_vm_resource = msg["resource"] == "com.cloud.vm.VirtualMachine"

        if is_create_event and is_vm_resource:
            vm = self._get_virtual_machine_data(
                msg["id"],
                msg["eventDateTime"]
            )

            if not vm:
                return
            update = {
                "action": "PATCH",
                "collection": "comp_unit",
                "type": "collections",
                "element": vm,
                "key": "globomap_%s" % vm['id']
            }
            return update

    def _get_virtual_machine_data(self, id, event_date=None):
        cloudstack_service = self._get_cloudstack_service()
        vm = cloudstack_service.get_virtual_machine(id)
        if vm:
            project = cloudstack_service.get_project(
                vm["projectid"]
            )
            account = project['account']
            element = {
                "id": "vm-%s" % vm["id"],
                "name": vm["name"],
                "timestamp": self._parse_date(event_date),
                "provider": "globomap",
                "properties": [
                    {
                        "key": "uuid",
                        "value": vm.get("id", ""),
                        "description": "UUID"
                    },
                    {
                        "key": "zone",
                        "value": vm.get("zonename", ""),
                        "description": "Zone name"
                    },
                    {
                        "key": "service_offering",
                        "value": vm.get("serviceofferingname", ""),
                        "description": "Compute Offering"
                    },
                    {
                        "key": "cpu_cores",
                        "value": vm.get("cpunumber", ""),
                        "description": "Number of CPU cores"
                    },
                    {
                        "key": "cpu_speed",
                        "value": vm.get("cpuspeed", ""),
                        "description": "CPU speed"
                    },
                    {
                        "key": "memory",
                        "value": vm.get("memory", ""),
                        "description": "RAM size"
                    },
                    {
                        "key": "template",
                        "value": vm.get("templatename", ""),
                        "description": "Template name"
                    },
                    {
                        "key": "project",
                        "value": vm.get("project", ""),
                        "description": "Project"
                    },
                    {
                        "key": "account",
                        "value": account,
                        "description": "Account"
                    },
                    {
                        "key": "environment",
                        "value": self.env,
                        "description": "Cloudstack Region"
                    },
                    {
                        "key": "creation_date",
                        "value": self._parse_date(vm["created"]),
                        "description": "Creation Date"
                    }
                ]
            }
            return element

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
