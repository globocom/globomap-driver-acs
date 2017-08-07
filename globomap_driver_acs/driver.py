import logging
import time
from dateutil.parser import parse
from cloudstack import CloudStackClient, CloudstackService
from rabbitmq import RabbitMQClient
from settings import ACS_RMQ_USER, ACS_RMQ_PASSWORD, \
    ACS_RMQ_HOST, ACS_RMQ_PORT, ACS_RMQ_QUEUE, \
    ACS_RMQ_VIRTUAL_HOST, ACS_API_URL, ACS_API_KEY, ACS_API_SECRET_KEY


class Cloudstack(object):

    log = logging.getLogger(__name__)

    def __init__(self):
        self.rabbitmq = RabbitMQClient(
            host=ACS_RMQ_HOST,
            port=ACS_RMQ_PORT,
            user=ACS_RMQ_USER,
            password=ACS_RMQ_PASSWORD,
            vhost=ACS_RMQ_VIRTUAL_HOST,
            queue_name=ACS_RMQ_QUEUE
        )

    def updates(self, number_messages=1):
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
            vm = self._get_virtual_machine_data(msg["entityuuid"], msg)
            if not vm:
                return
            update = {
                "action": "CREATE",
                "collection": "comp_unit",
                "type": "collections",
                "element": vm
            }
            return update

    def _get_virtual_machine_data(self, id, msg):
        cloudstack_service = self._get_cloudstack_service()
        virtual_machine = cloudstack_service.get_virtual_machine(id)
        if virtual_machine:
            project = cloudstack_service.get_project(
                virtual_machine["projectid"]
            )
            account = project['account']
            element = {
                "id": virtual_machine["displayname"],
                "name": virtual_machine["displayname"],
                "timestamp": self._get_event_time(msg["eventDateTime"]),
                "properties": [
                    {
                        "key": "uuid",
                        "value": virtual_machine["id"]
                    },
                    {
                        "key": "zone",
                        "value": virtual_machine["zonename"]
                    },
                    {
                        "key": "service_offering",
                        "value": virtual_machine["serviceofferingname"]
                    },
                    {
                        "key": "template",
                        "value": virtual_machine["templatename"]
                    },
                    {
                        "key": "project",
                        "value": virtual_machine["project"]
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
            ACS_API_URL, ACS_API_KEY,
            ACS_API_SECRET_KEY, True
        )
        return CloudstackService(acs_client)
