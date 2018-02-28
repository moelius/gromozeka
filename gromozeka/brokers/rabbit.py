import logging
import queue
import time

import pika
from pika.exceptions import ChannelClosed

from gromozeka.concurrency import Pool
from gromozeka.concurrency import ThreadWorker
from gromozeka.concurrency import commands
from gromozeka.concurrency import scheduler
from gromozeka.primitives.protocol import BrokerPointType, Request

CONSUMER_ID_FORMAT = "{exchange}_{exchange_type}_{queue}_{routing_key}"
SCHEDULER_CONSUMER_NAME = 'scheduler'
ETA_CONSUMER_NAME = 'eta'


# TODO extract to universal (BaseConsumer) - this is replacement solution
class Consumer:
    """Channel consumer

    Attributes:
        logger(:obj:`logging.Logger`): Class logger
        broker(:obj:`gromozeka.brokers.Consumer`): Broker
        exchange(:obj:`str`): Rabbitmq exchange
        exchange_type(:obj:`str`): : Rabbitmq exchange type
        exchange_type(:obj:`str`): : Rabbitmq exchange type
        queue_(:obj:`str`): Rabbitmq queue
        routing_key(:obj:`str`): : Rabbitmq routing key
        id(:obj:`str`): Consumer identification
        tasks(:obj:`dict` of :obj:`gromozeka.primitives.RegistryTask`): Consumer tasks
        prefetch_count(:obj:`int`): prefetch_count size
        _connection(:obj:`pika.SelectConnection`): pika connection
        _channel: Channel
        _closing(:obj:`bool`): Consumer state
        _consumer_tag(:obj:`int`): Consumer tag

    Args:
        broker(gromozeka.brokers.Broker): Broker
        connection(pika.SelectConnection): pika connection
        exchange(str): Rabbitmq exchange
        exchange_type(str): : Rabbitmq exchange type
        queue_(str): : Rabbitmq queue
        routing_key(str): : Rabbitmq routing key
    """

    def __init__(self, broker, connection, exchange, exchange_type, queue_, routing_key):
        self.broker = broker
        self.logger = logging.getLogger('gromozeka.broker.consumer')
        self.prefetch_count = 0
        self.exchange = exchange
        self.exchange_type = exchange_type
        self.queue = queue_
        self.routing_key = routing_key
        self.id = CONSUMER_ID_FORMAT.format(
            exchange=self.exchange,
            exchange_type=self.exchange_type,
            queue=self.queue,
            routing_key=self.routing_key)
        self._channel = None
        self._connection = connection
        self._closing = False
        self._consumer_tag = None
        self.tasks = {}

    @staticmethod
    def format_consumer_id_from_broker_point(broker_point):
        """Format consumer id from `primitives.BrokerPointType`

        Args:
            broker_point(gromozeka.BrokerPointType): Broker entry

        Returns:
            str: consumer identification
        """
        return CONSUMER_ID_FORMAT.format(
            exchange=broker_point.exchange,
            exchange_type=broker_point.exchange_type,
            queue=broker_point.queue,
            routing_key=broker_point.routing_key)

    def get_broker_point(self):
        """Get broker point

        Returns:
            gromozeka.BrokerPointType: Broker entry
        """
        return BrokerPointType(
            exchange=self.exchange,
            exchange_type=self.exchange_type,
            queue=self.queue,
            routing_key=self.routing_key)

    def add_task(self, task_id):
        """Add task to consumer queue

        Args:
            task_id(str): Registry task identification
        """
        from gromozeka import get_app
        atp = get_app()
        self.tasks[task_id] = atp.get_task(task_id)

    def open_channel(self):
        """Open channel

        """
        self.logger.debug('Creating a new channel')
        self._connection.channel(on_open_callback=self._on_channel_open)

    def on_message(self, _, basic_deliver, __, body):
        """Event when message received

        Args:
            _: channel
            basic_deliver:
            __:
            body:
        """
        self.logger.info('Received message # %s from %s: %s', basic_deliver.delivery_tag, self.id, body)
        from gromozeka import get_app
        atp = get_app()
        request = Request.from_json(json_message=body, delivery_tag=basic_deliver.delivery_tag)
        if self.queue in (atp.config.broker_retry_queue, atp.config.broker_eta_queue):
            if request.scheduler.every and not request.scheduler.eta:  # raw task
                delay = scheduler.delay(last_run=request.scheduler.eta,
                                        every=request.scheduler.every,
                                        interval=request.scheduler.interval,
                                        at=request.scheduler.at)
            else:
                delay = scheduler.delay_from_eta(
                    eta=request.scheduler.countdown if self.queue == atp.config.broker_retry_queue else request.scheduler.eta)
            if delay > 0:
                atp.scheduler.add(
                    wait_time=delay,
                    func=self.publish,
                    args=[request, request.broker_point.exchange, request.broker_point.routing_key],
                    priority=1 if self.queue == atp.config.broker_retry_queue else 2)
            else:
                self.publish(request, request.broker_point.exchange, request.broker_point.routing_key)
            self.acknowledge_message(basic_deliver.delivery_tag)
        else:
            request = Request.from_json(json_message=body, delivery_tag=basic_deliver.delivery_tag)
            if not request:
                return self.acknowledge_message(basic_deliver.delivery_tag)
            self.tasks[request.task.id].pool.in_queue.put(request)

    def publish(self, request, exchange=None, routing_key=None):
        """Publish request message to channel channel

        Args:
            request (gromozeka.primitives.Request):
            exchange(:obj:`str`, optional):
            routing_key(:obj:`str`, optional):
        """
        self._channel.basic_publish(exchange or self.exchange, routing_key or self.routing_key, request.to_json())

    def acknowledge_message(self, delivery_tag):
        """Acknowledge message by it`s delivery tag

        Args:
            delivery_tag(int):
        """
        self.logger.info('Acknowledging message %s', delivery_tag)
        self._channel.basic_ack(delivery_tag)

    def reject_message(self, delivery_tag, requeue=False):
        """Reject message by it`s delivery tag

        Args:
            delivery_tag(int):
            requeue(bool): If `True` message will reject with requeue
        """
        self.logger.info('Rejecting message %s', delivery_tag)
        self._channel.basic_reject(delivery_tag, requeue)

    def change_prefetch_count(self, new_size):
        if self._channel:
            self._channel.basic_qos(prefetch_count=new_size)
            self.prefetch_count = new_size
        else:
            self.prefetch_count = new_size

    def stop_consuming(self):
        """Stop consuming

        """
        if self._channel:
            self.logger.debug('Sending a Basic.Cancel RPC command to RabbitMQ')
            self._channel.basic_cancel(self._on_cancelok, self._consumer_tag)

    def add_on_cancel_callback(self):
        self.logger.debug('Adding consumer cancellation callback')
        self._channel.add_on_cancel_callback(self._on_consumer_cancelled)

    def _on_channel_open(self, channel):
        self.logger.debug('Channel opened')
        self._channel = channel
        self._add_on_channel_close_callback()
        self._setup_exchange(self.exchange)

    def _add_on_channel_close_callback(self):
        self.logger.debug('Adding channel close callback')
        self._channel.add_on_close_callback(self._on_channel_closed)

    def _on_channel_closed(self, channel, reply_code, reply_text):
        self.logger.debug('Channel %i was closed: (%s) %s',
                          channel, reply_code, reply_text)
        # self._connection.close()

    def _setup_exchange(self, exchange_name):
        self.logger.debug('Declaring exchange %s', exchange_name)
        self._channel.exchange_declare(self._on_exchange_declareok,
                                       exchange_name,
                                       self.exchange_type)

    def _on_exchange_declareok(self, _):
        self.logger.debug('Exchange declared')
        self._setup_queue(self.queue)

    def _setup_queue(self, queue_name):
        self.logger.debug('Declaring queue %s', queue_name)
        self._channel.queue_declare(self._on_queue_declareok, queue_name)

    def _on_queue_declareok(self, _):
        self.logger.debug('Binding %s to %s with %s',
                          self.exchange, self.queue, self.routing_key)
        self._channel.queue_bind(self._on_bindok, self.queue,
                                 self.exchange, self.routing_key)

    def _on_bindok(self, _):
        self.logger.debug('Queue bound')
        self._start_consuming()

    def _start_consuming(self):
        self.logger.debug('Issuing consumer related RPC commands')
        self._channel.basic_qos(prefetch_count=self.prefetch_count)
        self.add_on_cancel_callback()
        self._consumer_tag = self._channel.basic_consume(self.on_message, self.queue)

    def _on_consumer_cancelled(self, method_frame):
        self.logger.debug('Consumer was cancelled remotely, shutting down: %r',
                          method_frame)
        if self._channel:
            self._channel.close()

    def _on_cancelok(self, _):
        self.logger.debug('RabbitMQ acknowledged the cancellation of the consumer')
        self._close_channel()

    def _close_channel(self):
        self.logger.debug('Closing the channel')
        self._channel.close()


# TODO extract to universal (BaseBroker) - this is replacement solution
class Broker(Pool):
    """Broker class

    Attributes:
        logger(:obj:`logging.Logger`): Class logger
        config(gromozeka.config.Config): Config object
        `Worker`(:obj:`gromozeka.concurrency.Worker`): Parent class for `Customer`
        _url(:obj:`str`): Rabbitmq url
        _connection(:obj:`pika.SelectConnection`): pika connection
        _closing(:obj:`bool`): Broker state
        _consumers(:obj:`list` of :obj:`Consumer`): consumers

    """

    class Worker(ThreadWorker):
        retries = 0

    def __init__(self):
        self.logger = logging.getLogger("gromozeka.broker")
        from gromozeka import get_app
        self.config = get_app().config
        self.retries = 0
        worker = self.Worker
        worker.run = self.worker_run
        worker.logger = self.logger
        self._url = self.config.broker_url
        self._connection = None
        self._closing = False
        self._consumers = {}

        # add scheduler consumer
        self._add_consumer(
            broker_point=BrokerPointType(
                exchange=self.config.broker_retry_exchange,
                exchange_type='direct',
                queue=self.config.broker_retry_queue,
                routing_key=self.config.broker_retry_routing_key),
            name=SCHEDULER_CONSUMER_NAME)

        # add consumer for eta tasks
        self._add_consumer(
            broker_point=BrokerPointType(
                exchange=self.config.broker_eta_exchange,
                exchange_type='direct',
                queue=self.config.broker_eta_queue,
                routing_key=self.config.broker_eta_routing_key),
            name=ETA_CONSUMER_NAME)
        super().__init__(max_workers=1, worker_class=worker, logger=self.logger)

    def listen_cmd(self):
        """Listen to commands

        """
        while not self._stop_event.is_set():
            try:
                command, args = self.cmd_in_queue.get(True, 0.05)
            except queue.Empty:
                continue
            if command == commands.POOL_GROW:
                self._grow(args)
            elif command == commands.POOL_STOP:
                self.stop_()
            elif command == commands.BROKER_ADD_CONSUMER:
                self._add_consumer(task_id=args['task_id'], broker_point=args['broker_point'])
            elif command == commands.BROKER_PUBLISH:
                self._send_task(args)
            elif command == commands.BROKER_ACK:
                self._ack(broker_point=args['broker_point'], delivery_tag=args['delivery_tag'])
            elif command == commands.BROKER_REJECT:
                self._reject(broker_point=args['broker_point'], delivery_tag=args['delivery_tag'])
            elif command == commands.BROKER_PREFETCH_COUNT:
                self._prefetch_count(args['broker_point'], new_size=args['new_size'])

    @staticmethod
    def worker_run(self):
        """Overrides `Worker` method

        Args:
            self(gromozeka.brokers.rabbit.Broker.Worker): Parent `Worker` object
        """
        self.logger.info("start")
        from gromozeka import get_app
        app = get_app()
        while not self._stop_event.is_set():
            try:
                app.broker.serve()
                self.retries = 0
            except pika.exceptions.AMQPConnectionError:
                pass
            if self._stop_event.is_set():
                break
            if self.retries == app.config.broker_reconnect_max_retries:
                app.stop()
                break
            self.retries += 1
            time.sleep(app.config.broker_reconnect_retry_countdown)
            self.logger.info('Trying to reconnect')
        self.pool.out_queue.cancel_join_thread()
        self.logger.info("stop")

    def run(self):
        """Run `Broker` in thread or process

        """
        self.logger.info("start")
        self.grow(self.init_max_workers)
        self.listen_cmd()

    def serve(self):
        """Start `Broker`

        """
        if self._consumers:
            self._connection = self.connect()
            self._connection.ioloop.start()

    def send_task(self, request):
        """Send request to consumer

        Args:
            request(primitives.Request): `Request` object
        """
        self.cmd.put(commands.broker_publish(request=request))

    def _send_task(self, request):
        """

        Args:
            request(primitives.Request): `Request` object
        """
        if request.scheduler:
            if request.scheduler.countdown:
                consumer_id = SCHEDULER_CONSUMER_NAME
            else:
                consumer_id = ETA_CONSUMER_NAME
        else:
            consumer_id = Consumer.format_consumer_id_from_broker_point(request.broker_point)

        self._consumers[consumer_id].publish(request=request)

    def ack(self, broker_point, delivery_tag):
        """Acknowledge message with specific consumer by it's tag

        Args:
            broker_point(gromozeka.BrokerPointType): Broker entry
            delivery_tag(int): Original message delivery tag
        """
        self.cmd.put(commands.broker_ack(broker_point=broker_point, delivery_tag=delivery_tag))

    def _ack(self, broker_point, delivery_tag):
        consumer_id = Consumer.format_consumer_id_from_broker_point(broker_point=broker_point)
        consumer = self.get_consumer(consumer_id=consumer_id)
        consumer.acknowledge_message(delivery_tag=delivery_tag)

    def reject(self, broker_point, delivery_tag):
        """Reject message with specific consumer by it's tag

        Args:
            broker_point(gromozeka.BrokerPointType): Broker entry
            delivery_tag(int): Original message delivery tag
        """
        self.cmd.put(commands.broker_reject(broker_point=broker_point, delivery_tag=delivery_tag))

    def _reject(self, broker_point, delivery_tag):
        consumer_id = Consumer.format_consumer_id_from_broker_point(broker_point=broker_point)
        consumer = self.get_consumer(consumer_id=consumer_id)
        consumer.reject_message(delivery_tag=delivery_tag)

    def on_pool_size_change(self):
        from gromozeka import get_app
        atp = get_app()
        for cname, consumer in self._consumers.items():
            new_prefetch_count = 0
            for tname, task in atp.registry.items():
                if Consumer.format_consumer_id_from_broker_point(task.broker_point) == cname:
                    new_prefetch_count += len(task.pool.workers)
            if consumer.prefetch_count != new_prefetch_count:
                self.logger.info('prefetch count changed from %s to %s' % (consumer.prefetch_count, new_prefetch_count))
                try:
                    consumer.change_prefetch_count(new_prefetch_count)
                except ChannelClosed:
                    consumer.open_channel()
                    consumer.change_prefetch_count(new_prefetch_count)
                if consumer.prefetch_count == 0:
                    consumer.stop_consuming()

    def prefetch_count(self, broker_point, new_size):
        self.cmd.put(commands.broker_prefetch_count(broker_point=broker_point, new_size=new_size))

    def _prefetch_count(self, broker_point, new_size):
        consumer = self.get_consumer(consumer_id=Consumer.format_consumer_id_from_broker_point(broker_point))
        consumer.change_prefetch_count(new_size)

    def get_consumer(self, consumer_id):
        """Get consumer by it's id

        Args:
            consumer_id(str): consumer identification

        Returns:
            gromozeka.brokers.Consumer:
        """
        return self._consumers[consumer_id]

    def stop_(self):
        self._closing = True
        self._connection.close()
        super().stop_()

    def add_consumer(self, broker_point, task_id=None):
        """Add new `Consumer` to `Broker`

        Args:
            broker_point(primitives.BrokerPointType): broker entry
            task_id(str): task identification
        """
        self.cmd.put(commands.broker_add_consumer(task_id=task_id, broker_point=broker_point))

    def _add_consumer(self, broker_point, task_id=None, name=None):
        """

        Args:
            broker_point(primitives.BrokerPointType): Broker entry
            task_id(str): Task identification
            name(str): Consumer name
        """
        if name:
            consumer_id = name
        else:
            consumer_id = CONSUMER_ID_FORMAT.format(
                exchange=broker_point.exchange,
                exchange_type=broker_point.exchange_type,
                queue=broker_point.queue,
                routing_key=broker_point.routing_key)
        try:
            consumer = self._consumers[consumer_id]
        except KeyError:
            consumer = Consumer(
                broker=self,
                connection=self._connection,
                exchange=broker_point.exchange,
                exchange_type=broker_point.exchange_type,
                queue_=broker_point.queue,
                routing_key=broker_point.routing_key)

        if task_id:
            consumer.add_task(task_id)
        self._consumers[consumer_id] = consumer

    def connect(self):
        """Connect to Rabbitmq

        Returns:
            pika.SelectConnection:
        """
        self.logger.debug('Connecting to %s', self._url)
        return pika.SelectConnection(pika.URLParameters(self._url), self._on_connection_open, stop_ioloop_on_close=True)

    def _on_connection_open(self, _):
        self.logger.debug('Connection opened')
        self._add_on_connection_close_callback()
        for name, consumer in self._consumers.items():
            consumer._connection = self._connection
            consumer.open_channel()

    def _add_on_connection_close_callback(self):
        self.logger.debug('Adding connection close callback')
        self._connection.add_on_close_callback(self._on_connection_closed)

    def _on_connection_closed(self, _, reply_code, reply_text):
        if self._closing:
            self._connection.ioloop.stop()

    def _reconnect(self):
        # This is the old connection IOLoop instance, stop its ioloop
        self._connection.ioloop.stop()

        if not self._closing:
            # Create a new connection
            self._connection = self.connect()

            # There is now a new connection, needs a new ioloop to run
            self._connection.ioloop.start()

    def _close_connection(self):
        """This method closes the connection to RabbitMQ.

        """
        self.logger.debug('Closing connection')
        self._connection.close()
