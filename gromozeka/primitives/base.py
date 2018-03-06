import functools
import logging
import uuid

import gromozeka
import gromozeka.concurrency
import gromozeka.concurrency.scheduler
from gromozeka.primitives import TaskType, SchedulerType, Request

PENDING = 'PENDING'
RECEIVED = 'RECEIVED'
STARTED = 'STARTED'
SUCCESS = 'SUCCESS'
FAILURE = 'FAILURE'
CANCELED = 'CANCELED'
REJECTED = 'REJECTED'
RETRY = 'RETRY'


def task(bind=False, max_retries=0, retry_countdown=1, eta=None, every=None, interval=None, at=None):
    """Task decorator. Use to make usual function as task

    After this, you must register task

    Examples:

        Register task:

        .. code-block:: python

            @task()
            def hello_world():
                print('hello world')

            app=Gromozeka()

            app.register_task(
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
        eta(str): Datetime when to run task, formatted as "%Y-%m-%d %H:%M:%S.%f"
        every(:obj:`str`, optional): May be seconds, minutes, hours, days, weeks.
        interval(:obj:`int`, optional): Uses with `every`.
        at(:obj:`str`, optional): Time formatted as `hour:minute` or `hour:minute:second`.

    Returns:
        gromozeka.primitives.Task: task object
    """

    def task_decor(f):
        @functools.wraps(f)
        def wrapper():
            return Task(func=f, bind=bind, max_retries=max_retries, retry_countdown=retry_countdown, eta=eta,
                        every=every, interval=interval, at=at)

        return wrapper

    return task_decor


class RegistryTask:
    """Task registry class

    Attributes:
        id(:obj:`str`): Task identification
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
        id_(str): Task identification
        func: Task function
        bind(bool): If `True` `Task` will be in function arguments as first argument
        worker_class(gromozeka.concurrency.Worker): Task workers will be this type
        max_workers(int): Number of task workers
        retry_countdown(:obj:`int` or :obj:`float`): Pause between retries
    """

    __slots__ = ['id', 'func', 'bind', 'broker_point', 'worker_class', 'max_workers', 'max_retries', 'retry_countdown',
                 'pool']

    def __init__(self, broker_point, id_, func, bind, worker_class, max_workers, max_retries, retry_countdown):
        self.id = id_
        self.func = func
        self.bind = bind
        self.broker_point = broker_point
        self.worker_class = worker_class or gromozeka.concurrency.ThreadWorker
        self.max_workers = max_workers
        self.max_retries = max_retries
        self.retry_countdown = retry_countdown
        self.pool = gromozeka.concurrency.Pool(max_workers=self.max_workers, worker_class=self.worker_class)


class Task:
    """Task class.

    Attributes:
        uuid(:obj:`uuid.uuid4`, optional): Unique task runtime identification
        id(:obj:`str`): Task registry identification
        logger(:obj:`logging.Logger`): Class logger
        state(:obj:`str`): Current task state
        func: Task function
        args(:obj:`list`):Task function arguments
        kwargs(:obj:`dict`): Task function keyword arguments
        bind(:obj:`bool`): If `True` `Task` will be in function arguments as first argument
        max_retries(:obj:`int`): Maximum number of retries
        retry_countdown(:obj:`int` or :obj:`float`): Pause between retries
        retries(:obj:`int`): retries counter
        countdown(:obj:`str`): Retry countdown (%Y-%m-%d %H:%M:%S.%f)
        eta(:obj:`str`, optional): Task ETA (%Y-%m-%d %H:%M:%S.%f)
        broker_point(:obj:`gromozeka.primitives.BrokerPointType`): Broker entry
        delivery_tag(:obj:`int`): Original message delivery tag

    Args:
        func: Task function
        args(:obj:`list`, optional):Task function arguments
        kwargs(:obj:`dict`, optional): Task function keyword arguments
        bind(bool): If `True` `Task` will be in function arguments as first argument
        state(:obj:`str`, optional): Current task state
        max_retries(:obj:`int`, optional): Maximum number of retries
        retry_countdown(:obj:`int` or :obj:`float`, optional): Pause between retries
        retries(:obj:`int`, optional): retries counter
        countdown(:obj:`str`, optional): Retry countdown (%Y-%m-%d %H:%M:%S.%f)
        eta(:obj:`str`, optional): Task ETA (%Y-%m-%d %H:%M:%S.%f)
        uuid_(:obj:`uuid.uuid4`, optional): Unique task runtime identification
        broker_point(gromozeka.primitives.BrokerPointType): Broker entry
        delivery_tag(:obj:`int`, optional): Original message delivery tag
    """

    __slots__ = ['uuid', 'id', 'logger', 'func', 'args', 'kwargs', 'bind', 'max_retries', 'retry_countdown', 'retries',
                 'countdown', '_eta', 'broker_point', 'delivery_tag', 'state', '_every', '_interval', '_at']

    def __init__(self, func, args=None, kwargs=None, bind=None, state=PENDING, max_retries=0, retry_countdown=1,
                 retries=0, countdown='', eta='', uuid_=None, broker_point=None, delivery_tag=None, every=None,
                 interval=None, at=None):

        self.uuid = uuid_ or uuid.uuid4()
        self.id = "%s.%s" % (func.__module__, func.__name__)
        self.logger = logging.getLogger("gromozeka.pool.worker.{}".format(self.id))
        self.state = state
        self.func = func
        self.args = args or []
        self.kwargs = kwargs or {}
        self.bind = bind or False
        self.max_retries = max_retries
        self.retry_countdown = retry_countdown
        self.retries = retries
        self.countdown = countdown
        self._eta = eta
        self.broker_point = broker_point
        self.delivery_tag = delivery_tag
        self._every = every
        self._interval = interval
        self._at = at

    def register(self, broker_point, worker_class=None, max_workers=1, max_retries=0, retry_countdown=0):
        """Register task in task registry

        Args:
            broker_point(gromozeka.primitives.BrokerPointType): Broker entry
            worker_class(gromozeka.concurrency.Worker): Task workers will be this type
            max_workers(int): Number of task workers
            max_retries(int): Maximum number of retries
            retry_countdown(:obj:`int` or :obj:`float`): Pause between retries

        Returns:
            gromozeka.primitives.Task:
        """
        self.broker_point = broker_point
        r_task = RegistryTask(id_=self.id, func=self.func, bind=self.bind, worker_class=worker_class,
                              max_workers=max_workers, max_retries=max_retries or self.max_retries,
                              retry_countdown=retry_countdown or self.retry_countdown or 1,
                              broker_point=broker_point or self.broker_point)
        app = gromozeka.get_app()
        app.register_task(task=r_task, broker_point=self.broker_point)
        app.broker.task_register(task_id=self.id, broker_point=broker_point)
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
        return functools.partial(self.func, *(self, *args), **kwargs)()

    @property
    def request(self):
        """Task `Request` property

        Returns:
            gromozeka.primitives.Request:
        """
        task_ = TaskType(uuid=self.uuid.__str__(), id=self.id, args=self.args, kwargs=self.kwargs, retries=self.retries,
                         state=self.state, delivery_tag=self.delivery_tag)
        if self._eta or self.countdown:
            scheduler = SchedulerType(countdown=self.countdown, eta=self._eta, every=self._every,
                                      interval=self._interval, at=self._at)
        else:
            scheduler = None
        return Request(task=task_, broker_point=self.broker_point, scheduler=scheduler)

    @classmethod
    def from_request(cls, request):
        """Make `Task` from `Request`

        Args:
            request(gromozeka.primitives.Request): task `Request`

        Returns:
            gromozeka.primitives.Task:
        """
        app = gromozeka.get_app()
        r_task = app.get_task(request.task.id)
        return cls(func=r_task.func,
                   args=request.task.args,
                   kwargs=request.task.kwargs,
                   bind=r_task.bind,
                   max_retries=r_task.max_retries,
                   retry_countdown=r_task.retry_countdown,
                   retries=request.task.retries,
                   countdown=request.scheduler.countdown if request.scheduler else None,
                   eta=request.scheduler.eta if request.scheduler else None,
                   every=request.scheduler.every if request.scheduler else None,
                   interval=request.scheduler.interval if request.scheduler else None,
                   at=request.scheduler.at if request.scheduler else None,
                   uuid_=request.task.uuid,
                   broker_point=request.broker_point,
                   delivery_tag=request.task.delivery_tag)

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
        if self._every:
            delay = gromozeka.concurrency.scheduler.delay(last_run=self._eta, every=self._every,
                                                          interval=self._interval, at=self._at)
            self._eta = gromozeka.concurrency.scheduler.new_eta(delay)
        app = gromozeka.get_app()
        r_task = app.get_task(self.id)
        self.broker_point = r_task.broker_point
        self.args = args or self.args
        self.kwargs = kwargs or self.kwargs
        app.broker.task_send(request=self.request)

    def eta(self, datetime):
        """

        Args:
            datetime(str): Datetime formatted as "%Y-%m-%d %H:%M:%S.%f"

        Returns:
            gromozeka.primitives.Task:
        """
        self._eta = datetime
        return self

    def every(self, period):
        """

        Args:
            period(str): May be seconds, minutes, hours, days, weeks.

        Returns:
            gromozeka.primitives.Task:
        """
        self._every = period
        return self

    def interval(self, count):
        """

        Args:
            count(int): Period interval. Uses with every.

        Returns:
            gromozeka.primitives.Task:
        """
        self._interval = count
        return self

    def at(self, time):
        """

        Args:
            time(str): Time formatted as `hour:minute` or `hour:minute:second`.

        Returns:
            gromozeka.primitives.Task:
        """
        self._at = time
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

        if not e.max_retries:
            e.max_retries = self.max_retries
        if not e.retry_countdown:
            e.retry_countdown = self.retry_countdown
        if e.max_retries <= self.retries:
            return False
        self.state = RETRY
        self._retry(e)
        return True

    def on_success(self):
        """Event when task finishes successfully

        """
        if self.state == CANCELED:
            return
        self.state = SUCCESS
        self.retries = 0
        self._ack()
        if self._every:
            self.uuid = uuid.uuid4()
            self.apply_async(*self.args, **self.kwargs)

    def on_fail(self, e):
        """Event when task failed with `Exception`

        """
        self.state = FAILURE
        self._reject()
        raise e

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

        Returns:

        """
        self.logger.warning("retry on error: {0}".format(e))
        self.countdown = gromozeka.concurrency.scheduler.new_eta(seconds=e.retry_countdown)
        self.retries += 1
        app = gromozeka.get_app()
        app.broker.task_send(request=self.request)
        self._reject()

    def _ack(self):
        app = gromozeka.get_app()
        app.broker.task_done(broker_point=self.broker_point, delivery_tag=self.delivery_tag)

    def _reject(self):
        app = gromozeka.get_app()
        app.broker.task_reject(broker_point=self.broker_point, delivery_tag=self.delivery_tag)
