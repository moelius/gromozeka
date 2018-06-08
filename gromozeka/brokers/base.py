import logging
import queue

from gromozeka.concurrency import Pool, commands
from gromozeka.concurrency import ThreadWorker
from gromozeka.exceptions import GromozekaException


def get_broker_factory(app):
    """

    Args:
        app (gromozeka.Gromozeka):

    Returns:
        gromozeka.brokers.base.BrokerAdapter:
    """
    if app.config.broker_url.startswith('amqp://'):
        from gromozeka.brokers import RabbitMQPikaAdaptee
        broker_adapter = BrokerAdapter(RabbitMQPikaAdaptee(app=app))
    else:
        raise GromozekaException('unknown broker connection url')
    broker_adapter.broker.configure()
    return broker_adapter


class BrokerInterface:
    def __init__(self, app):
        """

        Args:
            app(gromozeka.Gromozeka):
        """
        self.logger = logging.getLogger("gromozeka.broker")
        self.app = app

    @staticmethod
    def worker_run(self):
        """This method will be in thread - start to work here

        """
        raise NotImplementedError

    def stop(self):
        """Stop broker

        """
        raise NotImplementedError

    def configure(self):
        """Configure broker

        """
        raise NotImplementedError

    def task_register(self, broker_point, task_id, options, deserializator):
        """

        Args:
            broker_point(gromozeka.primitives.protocol.ProtoBrokerPoint): Broker entry
            task_id(str): Task identification
            options: Specific broker options. See NatsOptions broker for example
            deserializator(gromozeka.primitives.base.TaskDeserializator):

        """
        raise NotImplementedError

    def task_send(self, request, broker_point, reply_to=None):
        """

        Args:
            request: serialized request
            broker_point(gromozeka.BrokerPoint):
            reply_to(gromozeka.BrokerPoint):

        """
        raise NotImplementedError

    def task_send_delayed(self, request, broker_point, delay):
        raise NotImplementedError

    def task_done(self, task_uuid, broker_point, delivery_tag):
        """

        Args:
            task_uuid:
            broker_point(gromozeka.primitives.protocol.ProtoBrokerPoint): Broker entry
            delivery_tag: Broker delivery tag (unique message identification)

        """
        raise NotImplementedError

    def task_reject(self, broker_point, delivery_tag):
        """

        Args:
            broker_point(gromozeka.primitives.protocol.ProtoBrokerPoint): Broker entry
            delivery_tag: Broker delivery tag (unique message identification)

        """
        raise NotImplementedError

    def on_pool_size_changed(self):
        """This method will be run, when worker pool size change

        """
        raise NotImplementedError

    def wait_for_start(self):
        """Not required method for check broker is started

        Returns:

        """
        pass


class BrokerAdapter(Pool):
    def __init__(self, broker):
        """

        Args:
            broker(BrokerInterface):
        """
        self.logger = logging.getLogger("gromozeka.broker")
        self.broker = broker
        self.worker = type('BrokerWorker', (ThreadWorker,),
                           {'app': self.broker.app, 'broker': self.broker, 'run': self.broker.worker_run,
                            'logger': self.logger})
        super().__init__(max_workers=1, worker_class=self.worker, logger=self.logger)

    def listen_cmd(self):
        """Listen to commands

        """
        while not self._stop_event.is_set():
            try:
                command, args = self.cmd.get(0.05)
            except queue.Empty:
                continue
            if command == commands.POOL_GROW:
                super()._grow(args)
            elif command == commands.POOL_STOP:
                self.broker.stop()
                super().stop_()
            elif command == commands.BROKER_TASK_REGISTER:
                self.broker.task_register(task_id=args['task_id'], broker_point=args['broker_point'],
                                          options=args['options'], deserializator=args['deserializator'])
            elif command == commands.BROKER_TASK_SEND:
                self.broker.task_send(args['request'], args['broker_point'],args['reply_to'])
            elif command == commands.BROKER_TASK_SEND_DELAYED:
                self.broker.task_send_delayed(args['request'], args['broker_point'], args['delay'])
            elif command == commands.BROKER_TASK_DONE:
                task_uuid, broker_point, delivery_tag = args['task_uuid'], args['broker_point'], args['delivery_tag']
                self.broker.task_done(task_uuid=task_uuid, broker_point=broker_point, delivery_tag=delivery_tag)
            elif command == commands.BROKER_TASK_REJECT:
                self.broker.task_reject(broker_point=args['broker_point'], delivery_tag=args['delivery_tag'])
            elif command == commands.BROKER_ON_POOL_SIZE_CHANGED:
                self.broker.on_pool_size_changed()

    def start(self):
        super().start()
        self.broker.wait_for_start()

    def task_register(self, broker_point, task_id, options=None, deserializator=None):
        """Register task in broker

        Args:
            broker_point(gromozeka.primitives.protocol.ProtoBrokerPoint): Broker entry
            task_id(str): Task identification
            options: Specific broker options. See NatsOptions broker for example

        """
        self.cmd.put(commands.broker_task_register(task_id=task_id, broker_point=broker_point, options=options,
                                                   deserializator=deserializator))

    def task_send(self, request, broker_point, reply_to=None):
        """

        Args:
            request: serialized request
            broker_point(gromozeka.BrokerPoint):

        """
        self.cmd.put(commands.broker_task_send(request=request, broker_point=broker_point, reply_to=reply_to))

    def task_send_delayed(self, request, broker_point, delay):
        """

        Args:
            request(gromozeka.primitives.protocol.Request): Request object
            broker_point:
            delay:

        """
        self.cmd.put(commands.broker_task_send_delayed(request=request, broker_point=broker_point, delay=delay))

    def task_done(self, task_uuid, broker_point, delivery_tag):
        """

        Args:
            task_uuid(str): task identification
            broker_point(gromozeka.primitives.protocol.ProtoBrokerPoint): Broker entry
            delivery_tag: Broker delivery tag (unique message identification)

        """
        self.cmd.put(
            commands.broker_task_done(task_uuid=task_uuid, broker_point=broker_point, delivery_tag=delivery_tag))

    def task_reject(self, broker_point, delivery_tag):
        """


        Args:
            broker_point(gromozeka.primitives.protocol.ProtoBrokerPoint): Broker entry
            delivery_tag: Broker delivery tag (unique message identification)

        """
        self.cmd.put(commands.broker_task_reject(broker_point=broker_point, delivery_tag=delivery_tag))

    def on_pool_size_changed(self):
        """This method will be run, when worker pool size change

        """
        self.cmd.put(commands.broker_on_pool_size_changed())
