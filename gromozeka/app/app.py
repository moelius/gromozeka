import logging
import signal
import sys

from gromozeka.brokers import Broker
from gromozeka.concurrency import Scheduler
from gromozeka.config import app_config
from gromozeka.exceptions import GromozekaException
from gromozeka.primitives import RegistryTask

DEFAULT_LOGGER_FORMAT = '%(asctime)-15s %(levelname)s %(name)s %(threadName)s %(processName)s %(message)s'


def get_app():
    """

    Returns:
         gromozeka.app.Gromozeka:
    """
    try:
        return atp
    except NameError:
        logging.getLogger("ATP").error('async task processor (ATP) not initialized')
        sys.exit(1)


class Gromozeka:
    """

    Attributes:
        logger(:obj:`logging.Logger`): Class logger
        broker(:obj:`gromozeka.brokers.Broker`): Broker
        scheduler(:obj:`gromozeka.concurrency.Scheduler`): Scheduler
        registry(:obj:`dict` of :obj:`gromozeka.primitives.Task`): Task registry
        config(:obj:`gromozeka.config.Config`): config object
    """

    __slots__ = ['logger', 'broker', 'scheduler', 'registry', 'config']

    def __init__(self):
        global atp
        atp = self
        self.config = app_config
        self.logger = logging.getLogger('gromozeka')
        self.broker = Broker(self)
        self.scheduler = Scheduler()
        self.registry = {}

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

        self.broker.start()
        self.scheduler.start()
        for name, task in self.registry.items():
            task.pool.start()

    def get_consumer(self, id_):
        """Get task consumer by task_id

        Args:
            id_(str): Unique task identification

        Returns:
             gromozeka.brokers.Consumer:
        """
        return self.broker.get_consumer(id_)

    def register_task(self, task, broker_point, worker_class=None, max_workers=1, max_retries=0, retry_countdown=0):
        """

        Args:
            task(:obj:`gromozeka.primitives.Task` or :obj:`gromozeka.primitives.RegistryTask`): Task
            broker_point(gromozeka.primitives.BrokerPointType): Broker entry
            worker_class(:class:`gromozeka.concurrency.Worker`, optional): Worker class
            max_workers(int): How match workers will start
            max_retries(int): Maximum number of retries, after it will reached task will down
            retry_countdown(int): Pause between retries (seconds)
        """
        if isinstance(task, RegistryTask):
            self.registry[task.id] = task
        else:
            task.register(broker_point=broker_point, worker_class=worker_class, max_workers=max_workers,
                          max_retries=max_retries, retry_countdown=retry_countdown)

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
            raise GromozekaException('task `{}` not registered in ATP registry'.format(id_))

    def _signal_handler(self, _, __):
        """Signal handler

        """
        self.stop()

    def stop(self):
        self.logger.debug('trying to shutdown graceful')
        self.scheduler.stop()
        for uuid, task in self.registry.items():
            task.pool.stop()
        self.broker.stop()
