import logging
import queue

from gromozeka.concurrency import Pool, commands
from gromozeka.concurrency import ThreadWorker
from gromozeka.exceptions import GromozekaException


class BrokerInterface:
    def __init__(self, app):
        self.logger = logging.getLogger("gromozeka.broker")
        self.app = app

    @staticmethod
    def worker_run(self):
        """This method will be in thread - start to work here

        """
        raise NotImplemented

    def stop(self):
        raise NotImplemented

    def configure(self):
        raise NotImplemented

    def task_register(self, broker_point, task_id):
        raise NotImplemented

    def task_send(self, request):
        raise NotImplemented

    def task_done(self, broker_point, delivery_tag):
        raise NotImplemented

    def task_reject(self, broker_point, delivery_tag):
        raise NotImplemented

    def on_pool_size_changed(self):
        raise NotImplemented


class BrokerAdapter(Pool):

    def __init__(self, app):
        """

        Args:
            app(gromozeka.Gromozeka):
        """
        self.logger = logging.getLogger("gromozeka.broker")
        self.app = app
        self.broker = self.get_broker_factory()
        self.broker.configure()
        self.worker = type('BrokerWorker', (ThreadWorker,),
                           {'app': self.app, 'broker': self.broker, 'run': self.broker.worker_run,
                            'logger': self.logger})
        super().__init__(max_workers=1, worker_class=self.worker, logger=self.logger)

    def get_broker_factory(self):
        """

        Returns:
            gromozeka.brokers.base.BrokerInterface:
        """
        if self.app.config.broker_url.startswith('amqp://'):
            from gromozeka.brokers import RabbitMQPikaAdaptee
            broker = RabbitMQPikaAdaptee(app=self.app)
        else:
            raise GromozekaException('unknown broker connection url')
        broker.configure()
        return broker

    def listen_cmd(self):
        """Listen to commands

        """
        while not self._stop_event.is_set():
            try:
                command, args = self.cmd_in_queue.get(True, 0.05)
            except queue.Empty:
                continue
            if command == commands.POOL_GROW:
                super()._grow(args)
            elif command == commands.POOL_STOP:
                self.broker.stop()
                super().stop_()
            elif command == commands.BROKER_TASK_REGISTER:
                self.broker.task_register(task_id=args['task_id'], broker_point=args['broker_point'])
            elif command == commands.BROKER_TASK_SEND:
                self.broker.task_send(args)
            elif command == commands.BROKER_TASK_DONE:
                self.broker.task_done(broker_point=args['broker_point'], delivery_tag=args['delivery_tag'])
            elif command == commands.BROKER_TASK_REJECT:
                self.broker.task_reject(broker_point=args['broker_point'], delivery_tag=args['delivery_tag'])
            elif command == commands.BROKER_ON_POOL_SIZE_CHANGED:
                self.broker.on_pool_size_changed()

    def task_register(self, broker_point, task_id):
        self.cmd.put(commands.broker_task_register(task_id=task_id, broker_point=broker_point))

    def task_send(self, request):
        self.cmd.put(commands.broker_task_send(request=request))

    def task_done(self, broker_point, delivery_tag):
        self.cmd.put(commands.broker_task_done(broker_point=broker_point, delivery_tag=delivery_tag))

    def task_reject(self, broker_point, delivery_tag):
        self.cmd.put(commands.broker_task_reject(broker_point=broker_point, delivery_tag=delivery_tag))

    def on_pool_size_changed(self):
        self.cmd.put(commands.broker_on_pool_size_changed())
