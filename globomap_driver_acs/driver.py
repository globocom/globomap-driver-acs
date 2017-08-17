import logging
import time
from dateutil.parser import parse
from cloudstack import CloudStackClient, CloudstackService
from rabbitmq import RabbitMQClient
from settings import get_setting


class Cloudstack(object):

    log = logging.getLogger(__name__)

    def __init__(self, params):
        self.env = params.get('env')

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
            try:
                message = self.rabbitmq.get_message()
                if message:
                    update = self._format_update(message)

            except StopIteration:
                if messages:
                    yield messages
                raise StopIteration
            else:
                if update:
                    messages.append(update)
                if len(messages) == number_messages:
                    yield messages
                    messages = []

                if not update:
                    break

    def _format_update(self, msg):
        create_event = msg["event"] == "VM.CREATE"
        event_completed = msg["status"] == "Completed"

        if event_completed and create_event:
            vm = self._get_virtual_machine_data(msg["entityuuid"])
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

    def _get_virtual_machine_data(self, id):
        cloudstack_service = self._get_cloudstack_service()
        virtual_machine = cloudstack_service.get_virtual_machine(id)
        if virtual_machine:
            project = cloudstack_service.get_project(
                virtual_machine["projectid"]
            )
            account = project['account']
            element = {
                "id": "vm-%s" % virtual_machine["id"],
                "name": virtual_machine["displayname"],
                "timestamp": self._get_event_time(virtual_machine["created"]),
                "provider": "globomap",
                "properties": [
                    {
                        "key": "uuid",
                        "value": virtual_machine.get("id", "")
                    },
                    {
                        "key": "zone",
                        "value": virtual_machine.get("zonename", "")
                    },
                    {
                        "key": "service_offering",
                        "value": virtual_machine.get("serviceofferingname", "")
                    },
                    {
                        "key": "cpu_cores",
                        "value": virtual_machine.get("cpunumber", "")
                    },
                    {
                        "key": "cpu_speed",
                        "value": virtual_machine.get("cpuspeed", "")
                    },
                    {
                        "key": "memory",
                        "value": virtual_machine.get("memory", "")
                    },
                    {
                        "key": "template",
                        "value": virtual_machine.get("templatename", "")
                    },
                    {
                        "key": "project",
                        "value": virtual_machine.get("project", "")
                    },
                    {
                        "key": "account",
                        "value": account
                    }
                ]
            }
            return element

    def _get_event_time(self, event_time):
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
