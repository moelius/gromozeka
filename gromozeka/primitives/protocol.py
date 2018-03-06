import json
from typing import NamedTuple

import gromozeka


class TaskType(NamedTuple):
    """Helper class

    """
    uuid: str
    id: str
    args: list = []
    kwargs: dict = {}
    state: str = 'PENDING'
    retries: int = 0
    delivery_tag: int = 0


class BrokerPointType(NamedTuple):
    """Helper class

    """
    exchange: str
    exchange_type: str
    queue: str
    routing_key: str


class SchedulerType(NamedTuple):
    """Helper class

    """
    eta: str = ''
    every: str = ''
    interval: str = ''
    at: str = ''
    countdown: str = ''


class MessageType(NamedTuple):
    """Helper class

    """
    task: TaskType
    scheduler: SchedulerType = None
    broker_point: BrokerPointType = None


class Request:
    """Request class. Use as task representation for Broker

    Attributes:
        task(gromozeka.primitives.TaskType): Task tuple
        broker_point(gromozeka.primitives.BrokerPointType): Broker entry tuple
        scheduler(gromozeka.primitives.SchedulerType): Scheduler tuple

    Args:
        task(gromozeka.primitives.TaskType): Task tuple
        broker_point(gromozeka.primitives.BrokerPointType): Broker entry tuple
        scheduler(gromozeka.primitives.SchedulerType): Scheduler tuple
    """
    __slots__ = ['task', 'broker_point', 'scheduler']

    def __init__(self, task, broker_point, scheduler=None):
        self.task = task
        self.broker_point = broker_point
        self.scheduler = scheduler

    def to_json(self):
        """Convert `Request` to json

        Returns:
            Json representation of `Request`
        """
        return json.dumps(MessageType(task=self.task, broker_point=self.broker_point, scheduler=self.scheduler))

    @classmethod
    def from_json(cls, json_message, state=None, delivery_tag=None):
        """Make `Request` from json

        Args:
            json_message(:obj:`str`, optional): `Request` json string
            state(:obj:`str`, optional): `TaskType` new state
            delivery_tag(:obj:`int`, optional): Original mess

        Returns:
            primitives.Request: `Request` object
        """
        request = json.loads(json_message)

        task_raw = request[0]
        task_uuid = task_raw[0]
        task_id = task_raw[1]
        task_args = task_raw[2]
        task_kwargs = task_raw[3]
        task_state = task_raw[4] if len(task_raw) > 4 else None
        task_retries = task_raw[5] if len(task_raw) > 5 else None
        task_delivery_tag = task_raw[6] if len(task_raw) > 6 else None
        task = TaskType(
            uuid=task_uuid,
            id=task_id,
            state=state or task_state,
            args=task_args,
            kwargs=task_kwargs,
            retries=task_retries,
            delivery_tag=delivery_tag or task_delivery_tag)

        if len(request) > 1 and request[1]:
            scheduler = SchedulerType(*request[1])
        else:
            scheduler = None

        if len(request) > 2 and request[2]:
            broker_point = BrokerPointType(*request[2])
        else:
            broker_point = gromozeka.get_app().get_task(task_id).broker_point
        return cls(task=task, broker_point=broker_point, scheduler=scheduler)
