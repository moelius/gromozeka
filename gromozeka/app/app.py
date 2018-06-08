import logging
import multiprocessing
import os
import signal
import sys

from gromozeka.backends import get_backend_factory
from gromozeka.brokers import get_broker_factory
from gromozeka.concurrency import Scheduler
from gromozeka.config import Config
from gromozeka.exceptions import GromozekaException
from gromozeka.primitives import RegistryTask


def get_app():
    """

    Returns:
         gromozeka.app.Gromozeka:
    """
    try:
        return app
    except NameError:
        logging.getLogger("gromozeka").error("Gromozeka's  application not initialized")
        sys.exit(1)


class Gromozeka:
    """

    Attributes:
        logger(:obj:`logging.Logger`): Class logger
        registry(:obj:`dict` of :obj:`gromozeka.primitives.Task`): Task registry
        config(:obj:`gromozeka.config.Config`): config object
        _broker_adapter(:obj:`gromozeka.brokers.BrokerAdapter`): Broker
        _backend_adapter(:obj:`gromozeka.backends.BackendAdapter`): Broker
        _scheduler(:obj:`gromozeka.concurrency.Scheduler`): Scheduler
    """

    __slots__ = ['logger', '_broker_adapter', '_backend_adapter', '_scheduler', 'registry', 'config', 'pid',
                 '_exit_signum', 'is_closing']

    def __init__(self):
        global app
        app = self
        self.config = Config()
        self.logger = logging.getLogger('gromozeka')
        self._broker_adapter = None
        self._backend_adapter = None
        self._scheduler = None
        self.registry = {}
        self.pid = multiprocessing.current_process().pid
        self._exit_signum = 0
        self.is_closing = False

    @property
    def broker_adapter(self):
        if not self._broker_adapter:
            self._broker_adapter = get_broker_factory(self)
        return self._broker_adapter

    @property
    def backend_adapter(self):
        if not self._backend_adapter:
            self._backend_adapter = get_backend_factory(self)
        return self._backend_adapter

    @property
    def scheduler(self):
        if not self._scheduler:
            self._scheduler = Scheduler()
        return self._scheduler

    def config_from_env(self):
        """Configure Gromozeka with environment variables

        Examples:
            .. code-block:: python

                app=Gromozeka().config_from_env()

        Returns:
            gromozeka.Gromozeka: Configured application
        """
        self.config.from_env()
        return self

    def config_from_dict(self, conf):
        """Configure Gromozeka with :obj:`dict`

        Examples:
            .. code-block:: python

                conf={'app_prefix':'my_application','broker_reconnect_max_retries':3}
                app=Gromozeka().config_from_dict(conf)

        Args:
            conf(dict): config :obj:`dict`

        Returns:
            gromozeka.Gromozeka: Configured application
        """
        self.config.from_dict(conf)
        return self

    def start(self):
        """Start application

        """
        # setup signal handler
        for sig in ('SIGTERM', 'SIGINT'):
            signal.signal(getattr(signal, sig), self._signal_handler)
        self.backend_adapter.start()
        for _, registry_task in self.registry.items():
            if not hasattr(registry_task, 'pool'):
                continue
            registry_task.pool.start()
        self.broker_adapter.start()

    def register(self, task, broker_point, worker_class=None, max_workers=1, max_retries=0, retry_countdown=0,
                 ignore_result=False, broker_options=None, deserializator=None):
        """

        Args:
            task(:obj:`gromozeka.primitives.Task` or :obj:`gromozeka.primitives.RegistryTask`): Task
            broker_point(gromozeka.primitives.BrokerPointType): Broker entry
            worker_class(:class:`gromozeka.concurrency.Worker`, optional): Worker class
            max_workers(int): How match workers will start
            max_retries(int): Maximum number of retries, after it will reached task will down
            retry_countdown(int): Pause between retries (seconds)
            ignore_result(bool): If True result will be saved in result backend
            broker_options: Specific broker options. See NatsOptions broker for example
            deserializator(gromozeka.primitives.base.TaskDeserializator):
        """
        if isinstance(task, RegistryTask):
            self.registry[task.task_id] = task
        else:
            task.register(broker_point=broker_point, worker_class=worker_class, max_workers=max_workers,
                          max_retries=max_retries, retry_countdown=retry_countdown, ignore_result=ignore_result,
                          broker_options=broker_options, deserializator=deserializator)

    def get_task(self, id_):
        """Get task by id

        Args:
            id_(str): Unique task identification

        Returns:
            gromozeka.primitives.RegistryTask: registry task
        """
        try:
            return self.registry[id_]
        except KeyError:
            raise GromozekaException('task `{}` not registered in registry'.format(id_))

    def _signal_handler(self, signum, _):
        """Signal handler

        """
        if multiprocessing.current_process().pid == self.pid:
            self.stop()
            if self._exit_signum:
                os._exit(signum)

    def stop_signal(self, signum):
        self._exit_signum = signum
        os.kill(self.pid, signum)

    def stop(self):
        self.logger.info('trying graceful shutdown')
        self.is_closing = True
        [rt.pool.stop() for rt in self.registry.values() if hasattr(rt, 'pool')]
        self.backend_adapter.stop()
        self.broker_adapter.stop()
        os._exit(0)
