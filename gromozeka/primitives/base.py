import functools
import json
import logging
import uuid
from collections import deque

import gromozeka
import gromozeka.concurrency
from gromozeka.exceptions import SerializationError
from gromozeka.primitives.dag import Vertex, Graph, GROUP, CHAIN, ACHAIN
from gromozeka.primitives.task_pb2 import Task as ProtoTask, ReplyToBrokerPoint

PENDING = 'PENDING'
RECEIVED = 'RECEIVED'
STARTED = 'STARTED'
SUCCESS = 'SUCCESS'
FAILURE = 'FAILURE'
CANCELED = 'CANCELED'
REJECTED = 'REJECTED'
RETRY = 'RETRY'


def task(bind=False, max_retries=0, retry_countdown=1, ignore_result=None, reply_to=None):
    """Task decorator. Use to make usual function as task

    After this, you must register task

    Examples:

        Register task:

        .. code-block:: python

            @task()
            def hello_world():
                print('hello world')

            app=Gromozeka()

            app.register(
                hello_world(),
                broker_point=BrokerPointType(
                    exchange="first_exchange",
                    exchange_type='direct',
                    queue='first_queue',
                    routing_key='first'))

            app.start()

        or :obj:`Task` object can register self:

        .. code-block:: python

            @task()
            def hello_world():
                print('hello world')

            app=Gromozeka()

            hello_world().register(
                broker_point=BrokerPointType(
                    exchange="first_exchange",
                    exchange_type='direct',
                    queue='first_queue',
                    routing_key='first'))

            app.start()

    Args:
        bind(:obj:`bool`, optional): If `True` `Task` will be in function arguments as first argument
        max_retries(:obj:`int`, optional): Maximum number of retries
        retry_countdown(:obj:`int` or :obj:`float`, optional): Pause between retries
        ignore_result(bool): If False result will be saved in result backend
        reply_to(BrokerPoint):

    Returns:
        gromozeka.primitives.Task: task object
    """

    def task_decor(f):
        @functools.wraps(f)
        def wrapper(*args, **kwargs):
            if reply_to:
                reply_to_exchange, reply_to_routing_key = reply_to.exchange, reply_to.routing_key
            else:
                reply_to_exchange, reply_to_routing_key = None, None
            return Task(func=f, bind=bind, max_retries=max_retries, retry_countdown=retry_countdown,
                        ignore_result=ignore_result, reply_to_exchange=reply_to_exchange,
                        reply_to_routing_key=reply_to_routing_key, args=args, kwargs=kwargs)

        return wrapper

    return task_decor


class RegistryTask:
    """Task registry class

    Attributes:
        task_id(:obj:`str`): Task identification
        func: Task function
        bind(:obj:`bool`): If `True` `Task` will be in function arguments as first argument
        broker_point(:obj:`gromozeka.primitives.BrokerPointType`): Broker entry
        worker_class(:obj:`gromozeka.concurrency.Worker`): Task workers will be this type
        max_workers(:obj:`int`): Number of task workers
        max_retries(:obj:`int`): Maximum number of retries
        retry_countdown(:obj:`int` or :obj:`float`): Pause between retries
        pool(:obj:`gromozeka.concurrency.Pool`): Task workers pool
    Args:
        broker_point(gromozeka.primitives.BrokerPointType): Broker entry
        task_id(str): Task identification
        func: Task function
        bind(bool): If `True` `Task` will be in function arguments as first argument
        worker_class(gromozeka.concurrency.Worker): Task workers will be this type
        max_workers(int): Number of task workers
        retry_countdown(:obj:`int` or :obj:`float`): Pause between retries
    """

    __slots__ = ['task_id', 'func', 'bind', 'broker_point', 'worker_class', 'max_workers', 'max_retries',
                 'retry_countdown', 'pool', 'ignore_result', 'deserializator']

    def __init__(self, broker_point, task_id, func, bind, worker_class, max_workers, max_retries, retry_countdown,
                 ignore_result):
        self.task_id = task_id
        self.func = func
        self.bind = bind
        self.broker_point = broker_point
        self.worker_class = worker_class or gromozeka.concurrency.ThreadWorker
        self.max_workers = max_workers
        self.max_retries = max_retries
        self.retry_countdown = retry_countdown
        self.ignore_result = ignore_result
        self.pool = gromozeka.concurrency.Pool(max_workers=self.max_workers, worker_class=self.worker_class)


class BrokerPoint:
    def __init__(self, exchange=None, exchange_type=None, queue=None, routing_key=None):
        self.exchange = exchange
        self.exchange_type = exchange_type
        self.queue = queue
        self.routing_key = routing_key

    def __str__(self):
        return '_'.join([v for v in to_dict(self).values() if v])


class TaskDeserializator:

    def deserialize(self, raw_task):
        """

        Args:
            raw_task: raw task to deserialize

        Returns:
            task properties, tuple task_id args kwargs uuid graph_uuid
        """
        raise NotImplementedError


class Task(Vertex):
    """Task class.

    Args:
        func: Task function
        args(:obj:`list`, optional):Task function arguments
        kwargs(:obj:`dict`, optional): Task function keyword arguments
        bind(bool): If `True` `Task` will be in function arguments as first argument
        state(:obj:`str`, optional): Current task state
        max_retries(:obj:`int`, optional): Maximum number of retries
        retry_countdown(:obj:`int` or :obj:`float`, optional): Pause between retries
        retries(:obj:`int`, optional): retries counter
        delay(:obj:`str`, optional): Retry countdown (%Y-%m-%d %H:%M:%S.%f)
        uuid_(:obj:`uuid.uuid4`, optional): Unique task runtime identification
        broker_point(gromozeka.primitives.BrokerPointType): Broker entry
        delivery_tag(:obj:`int`, optional): Original message delivery tag
    """

    __slots__ = ['uuid', 'task_id', 'logger', 'func', 'args', 'kwargs', 'bind', 'app', 'max_retries', 'retry_countdown',
                 'retries', 'delay', 'broker_point', 'delivery_tag', 'state', 'manual_ack',
                 'ignore_result', 'graph_uuid', 'reply_to_exchange', 'reply_to_routing_key']

    def __init__(self, func, args=None, kwargs=None, bind=None, app=None, state=None, max_retries=None,
                 retry_countdown=None, retries=None, delay=None, uuid_=None, broker_point=None, delivery_tag=None,
                 ignore_result=None, graph_uuid=None, reply_to_exchange=None, reply_to_routing_key=None):
        self.app = app or gromozeka.get_app()
        self.uuid = uuid_
        self.task_id = "%s.%s" % (func.__module__, func.__name__)
        self.logger = logging.getLogger("gromozeka.pool.worker.{}".format(self.task_id))
        self.state = state or PENDING
        self.func = func
        self.args = args or []
        self.kwargs = kwargs or {}
        self.bind = bind or False
        self.max_retries = max_retries or 1
        self.retry_countdown = retry_countdown or 0
        self.retries = retries or 0
        self.delay = delay or 0
        self.broker_point = broker_point
        self.delivery_tag = delivery_tag or 0
        self.manual_ack = False
        self.ignore_result = ignore_result or False
        self.graph_uuid = graph_uuid
        self.error = None
        self.exc = None
        self.reply_to_exchange = reply_to_routing_key
        self.reply_to_routing_key = reply_to_exchange
        super().__init__(uuid=self.uuid, task_id=self.task_id, args=self.args, kwargs=self.kwargs, bind=self.bind,
                         reply_to_exchange=reply_to_exchange, reply_to_routing_key=reply_to_routing_key)

    def register(self, broker_point, worker_class=None, max_workers=1, max_retries=0, retry_countdown=0,
                 ignore_result=False, broker_options=None, deserializator=None):
        """Register task in task registry

        Args:
            broker_point(gromozeka.primitives.ProtoBrokerPoint): Broker entry
            worker_class(:class:`gromozeka.concurrency.Worker`): Task workers will be this type
            max_workers(int): Number of task workers
            max_retries(int): Maximum number of retries
            retry_countdown(:obj:`int` or :obj:`float`): Pause between retries
            ignore_result(bool): If False result will be saved in result backend
            broker_options: Specific broker options. See NatsOptions broker for example
            deserializator(TaskDeserializator): Custom task deserializator

        Returns:
            gromozeka.primitives.Task:
        """
        if deserializator:
            if not isinstance(deserializator, TaskDeserializator):
                raise SerializationError('custom deserializator should be inherited from `TaskDeserializator`')
        self.broker_point = broker_point
        r_task = RegistryTask(task_id=self.task_id, func=self.func, bind=self.bind, worker_class=worker_class,
                              max_workers=max_workers, max_retries=max_retries or self.max_retries,
                              retry_countdown=retry_countdown or self.retry_countdown or 1,
                              broker_point=broker_point or self.broker_point, ignore_result=ignore_result)
        self.app.register(task=r_task, broker_point=self.broker_point)
        self.app.broker_adapter.task_register(task_id=self.task_id, broker_point=broker_point, options=broker_options,
                                              deserializator=deserializator)
        return self

    def __call__(self, *args, **kwargs):
        """Execute `Task` function

        Examples:

            .. code-block:: python

                @task()
                def some_task(a,b):
                    print(a+b)

                some_task()(1,2)

        Args:
            *args: function arguments
            **kwargs: function keyword arguments

        Returns:
            function result
        """
        if self.bind:
            args[0:0] = [self]
        return functools.partial(self.func, *args, **kwargs)()

    @property
    def request(self):
        """Task `Request` property

        Returns:
            gromozeka.primitives.Request:
        """
        args = json.dumps(self.args) if self.args else None
        kwargs = json.dumps(self.kwargs) if self.kwargs else None
        if not self.broker_point:
            self.broker_point = self.app.get_task(self.task_id).broker_point
        if self.reply_to_exchange:
            reply_to = ReplyToBrokerPoint(exchange=self.reply_to_exchange,
                                          routing_key=self.reply_to_routing_key)
        else:
            reply_to = None
        return ProtoTask(uuid=self.uuid.__str__(), task_id=self.task_id, args=args, kwargs=kwargs, retries=self.retries,
                         delay=self.delay, state=self.state, delivery_tag=self.delivery_tag, graph_uuid=self.graph_uuid,
                         reply_to=reply_to)

    @classmethod
    def from_proto(cls, raw_task, delivery_tag=None, deserializator=False):
        """Make `Task` from `Request`

        Args:
            raw_task(gromozeka.primitives.Request): task `Request`
            delivery_tag(int): use with deserializator, this arg from broker
            deserializator(TaskDeserializator):

        Returns:
            gromozeka.primitives.Task:
        """
        app = gromozeka.get_app()
        if not deserializator:
            proto_task = ProtoTask()
            proto_task.ParseFromString(raw_task)
            task_uuid = proto_task.uuid
            graph_uuid = proto_task.graph_uuid
            task_id = proto_task.task_id
            args = None if not proto_task.args else json.loads(proto_task.args)
            kwargs = None if not proto_task.kwargs else json.loads(proto_task.kwargs)
            delay = proto_task.delay
            delivery_tag = proto_task.delivery_tag
            retries = proto_task.retries
            reply_to_exchange, reply_to_routing_key = proto_task.reply_to.exchange, proto_task.reply_to.routing_key

        else:
            task_uuid, task_id, graph_uuid, args, kwargs, \
            retries, delay, reply_to_exchange, reply_to_routing_key = deserializator.deserialize(raw_task=raw_task)
        r_task = app.get_task(task_id)
        return cls(func=r_task.func,
                   args=args,
                   kwargs=kwargs,
                   bind=r_task.bind,
                   app=app,
                   max_retries=r_task.max_retries,
                   retry_countdown=r_task.retry_countdown,
                   retries=retries,
                   delay=delay,
                   uuid_=task_uuid,
                   delivery_tag=delivery_tag,
                   ignore_result=r_task.ignore_result,
                   graph_uuid=graph_uuid,
                   broker_point=BrokerPoint(exchange=r_task.broker_point.exchange,
                                            exchange_type=r_task.broker_point.exchange_type,
                                            queue=r_task.broker_point.queue,
                                            routing_key=r_task.broker_point.routing_key),
                   reply_to_exchange=reply_to_exchange,
                   reply_to_routing_key=reply_to_routing_key)

    def apply_async(self, *args, **kwargs):
        """Run `Task` asynchronously

        Examples:

            .. code-block:: python

                @task()
                def some_task(a,b):
                    print(a+b)

                some_task().apply_async(1,2)

        Args:
            *args: `Task` function arguments
            **kwargs: `Task` function keyword arguments
        """
        self.uuid = self.uuid or str(uuid.uuid4())
        r_task = self.app.get_task(self.task_id)
        self.broker_point = r_task.broker_point
        self.args = args or self.args
        self.kwargs = kwargs or self.kwargs
        self.app.broker_adapter.task_send(request=self.request.SerializeToString(), broker_point=self.broker_point)

    def a(self, *args):
        self.args = args
        return self

    def k(self, **kwargs):
        self.kwargs = kwargs
        return self

    def on_receive(self):
        """Event when task received in worker

        """
        self.state = RECEIVED

    def on_start(self):
        """Event when started tu run task

        """
        self.state = STARTED

    def on_reject(self):
        """Event when task rejected

        """
        self.state = REJECTED
        self._reject()

    def on_cancel(self):
        """Event when task canceled

        """
        self.state = CANCELED
        self._reject()

    def on_retry(self, e):
        """Event when task on retry

        Args:
            e(gromozeka.exceptions.Retry):

        Returns:
             bool: `True` if task will retry more times
        """

        self.state = RETRY
        if not e.max_retries:
            e.max_retries = self.max_retries
        if not e.retry_countdown:
            e.retry_countdown = self.retry_countdown
        if e.max_retries <= self.retries:
            return False
        if self.graph_uuid:
            graph_dict = self.app.backend_adapter.graph_get(self.graph_uuid)
            graph = Graph.from_dict(graph_dict)
            current = graph.vertex_by_uuid(self.uuid)
            graph.state = current.state = self.state
            current.error = self.error
            self.app.backend_adapter.graph_update(graph_uuid=graph.uuid,
                                                  verticies=[current.as_dict()],
                                                  graph_state=graph.state, error_task_uuid=current.uuid,
                                                  short_error=self.error)
        self._retry(e)
        return True

    def on_success(self, res):
        """Event when task finishes successfully

        """
        if self.state == CANCELED:
            return
        self.state = SUCCESS
        self.retries = 0
        # result save
        if not self.graph_uuid:
            self.app.backend_adapter.result_set(self.uuid, res)
            self._ack()
            return
        self.app.backend_adapter.result_set(self.uuid, res, graph_uuid=self.graph_uuid)
        graph_dict = self.app.backend_adapter.graph_get(self.graph_uuid)

        graph = Graph.from_dict(graph_dict)
        current = graph.vertex_by_uuid(self.uuid)
        current.state = self.state
        self.update_graph_states(graph, current, result=res)
        for v in graph.next_vertex(from_uuid=current.uuid):
            if v.args_from:
                if v.args_from == current.fr:
                    args_from = res
                elif graph.vertex_by_uuid(v.args_from).operation == GROUP:
                    args_from = self.app.backend_adapter.group_get_result(graph_uuid=graph.uuid, group_uuid=v.args_from)
                else:
                    args_from_parent_vertex = graph.vertex_by_uuid(v.args_from)
                    args_from_vertex = graph.vertex_by_uuid(args_from_parent_vertex.children[1]).uuid
                    args_from = self.app.backend_adapter.result_get(graph_uuid=graph.uuid,
                                                                    task_uuid=args_from_vertex)
                if v.bind:
                    v.args[0:0] = [args_from] if v.bind else [args_from] + v.args
                else:
                    v.args = [args_from] + v.args
            v.apply(graph.uuid)

        # acknowledge broker message
        self._ack()

    def update_graph_states(self, graph, vertex, graph_state=None, result=None, error=None):
        """

        Args:
            graph(gromozeka.primitives.dag.Graph): graph instance
            vertex(gromozeka.primitives.dag.Vertex): vertex instance
            graph_state:
            result: Graph vertex result
            error(str): error string

        Returns:
            list of gromozeka.primitives.dag.Vertex:
        """
        stack = deque([vertex])
        completed = []
        error_task_uuid = vertex.uuid if error else None
        while stack:
            current = stack[-1]
            op = current.operation
            if not op:
                current.state = vertex.state
                parent = graph.vertex_by_uuid(current.parent)
                if parent:
                    if parent.operation in (CHAIN, ACHAIN):
                        if graph.is_chain_tail(current):
                            stack.appendleft(parent)
                    else:
                        stack.appendleft(parent)
            elif op == GROUP:
                group_len = self.app.backend_adapter.group_add_result(
                    graph_uuid=graph.uuid,
                    group_uuid=current.uuid,
                    task_uuid=vertex.uuid,
                    result=result)
                if group_len != len(current.children):
                    stack.pop()
                    continue
                current.state = vertex.state
                parent = graph.vertex_by_uuid(current.parent)
                if parent:
                    if parent.operation in (CHAIN, ACHAIN):
                        if graph.is_chain_tail(current):
                            stack.appendleft(parent)
                    else:
                        stack.appendleft(parent)
            elif current.state == vertex.state:
                if current.parent:
                    stack.appendleft(graph.vertex_by_uuid(current.parent))
            elif graph.vertex_by_uuid(current.children[1]) in completed:
                graph.vertex_by_uuid(current.children[0]).state = vertex.state
                current.state = vertex.state
                if current.parent:
                    stack.appendleft(graph.vertex_by_uuid(current.parent))
            else:
                stack.pop()
                continue
            if current.uuid == graph.last_uuid:
                graph_state = vertex.state
            completed.append(current)
            stack.pop()
        self.app.backend_adapter.graph_update(graph_uuid=graph.uuid, verticies=[comp.as_dict() for comp in completed],
                                              graph_state=graph_state, error_task_uuid=error_task_uuid,
                                              short_error=error)

    def on_fail(self):
        """Event when task failed with `Exception`

        """
        self.state = FAILURE
        if self.graph_uuid:
            graph_dict = self.app.backend_adapter.graph_get(self.graph_uuid)
            graph = Graph.from_dict(graph_dict)
            current = graph.vertex_by_uuid(self.uuid)
            current.state = self.state
            current.error = self.error
            current.exc = self.exc
            self.update_graph_states(graph=graph, vertex=current, graph_state=self.state, error=self.error)
        self._reject()

    def cancel(self):
        """Cancel task. Use this method in `Task` function. Bind option must be `True`

        Examples:
            .. code-block:: python

                @task(bind=True)
                def some_task():
                    try:
                        a = 1 / 0
                    except ZeroDivisionError as e:
                        self.cancel()
        """
        self.on_cancel()

    def _retry(self, e):
        """

        Args:
            e(gromozeka.exceptions.Retry):

        """
        self.logger.warning("retry on error: {0}".format(e))
        self.delay = e.retry_countdown
        self.retries += 1
        self.app.broker_adapter.task_send_delayed(request=self.request.SerializeToString(),
                                                  broker_point=self.broker_point, delay=self.delay)
        self._reject()

    def _ack(self):
        if self.manual_ack:
            return
        self.app.broker_adapter.task_done(task_uuid=self.uuid, broker_point=self.broker_point,
                                          delivery_tag=self.delivery_tag)

    def _result_set(self, result):
        return self.app.backend_adapter.result_set(self.uuid, result)

    def _reject(self):
        self.app.broker_adapter.task_reject(broker_point=self.broker_point, delivery_tag=self.delivery_tag)


def to_dict(o):
    return {k: getattr(o, k) for k in o.__dir__() if
            not callable(getattr(o, k)) and not k.startswith('__')}
