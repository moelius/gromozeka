import logging
import time

import pika
import pika.exceptions

from gromozeka.brokers.base import BrokerInterface
from gromozeka.concurrency import scheduler
from gromozeka.primitives import Request
from gromozeka.primitives.protocol import BrokerPointType


class RabbitMQPikaAdaptee(BrokerInterface):
    _consumer_id_format = "{exchange}_{exchange_type}_{queue}_{routing_key}"
    _retry_consumer_name = 'scheduler'
    _eta_consumer_name = 'eta'

    def __init__(self, app):
        self.reconnect_max_retries = None
        self.reconnect_retry_countdown = None
        self._connection = None
        self._closing = False
        self._consumers = {}
        self._url = None
        self._retry_exchange = None
        self._retry_queue = None
        self._retry_routing_key = None
        self._eta_exchange = None
        self._eta_queue = None
        self._eta_routing_key = None
        super().__init__(app=app)

    def configure(self):
        self._url = self.app.config.broker_url
        self.reconnect_max_retries = self.app.config.broker_reconnect_max_retries
        self.reconnect_retry_countdown = self.app.config.broker_reconnect_retry_countdown
        self._retry_exchange = self.app.config.broker_retry_exchange
        self._retry_queue = self.app.config.broker_retry_queue
        self._retry_routing_key = self.app.config.broker_retry_routing_key
        self._eta_exchange = self.app.config.broker_eta_exchange
        self._eta_queue = self.app.config.broker_eta_queue
        self._eta_routing_key = self.app.config.broker_eta_routing_key

    @staticmethod
    def worker_run(self):
        self.logger.info('start')
        reconnect_retries = 0
        self.broker.add_internal_consumers()
        while not self._stop_event.is_set():
            try:
                self.broker.serve()
                reconnect_retries = 0
            except pika.exceptions.AMQPConnectionError:
                pass
            if self._stop_event.is_set():
                break
            if reconnect_retries == self.broker.reconnect_max_retries:
                self.app.stop()
                break
            reconnect_retries += 1
            time.sleep(self.broker.reconnect_retry_countdown)
            self.logger.info('Trying to reconnect')
        self.logger.info('stop')

    def stop(self):
        self._closing = True
        self._connection.close()

    def add_internal_consumers(self):
        consumer = Consumer(
            broker=self,
            id_=self._retry_consumer_name,
            connection=self._connection,
            exchange=self._retry_exchange,
            exchange_type='direct',
            queue_=self._retry_queue,
            routing_key=self._retry_routing_key)
        self._consumers[self._retry_consumer_name] = consumer

        consumer = Consumer(
            broker=self,
            id_=self._eta_consumer_name,
            connection=self._connection,
            exchange=self._eta_exchange,
            exchange_type='direct',
            queue_=self._eta_queue,
            routing_key=self._eta_routing_key)
        self._consumers[self._eta_consumer_name] = consumer

    def serve(self):
        """Start `Broker`

        """
        self._connection = self._connect()
        self._connection.ioloop.start()

    def task_register(self, broker_point, task_id):
        """

        Args:
            broker_point(gromozeka.primitives.BrokerPointType): Broker entry
            task_id(str): Task identification
        """
        consumer_id = self._consumer_id_format.format(
            exchange=broker_point.exchange,
            exchange_type=broker_point.exchange_type,
            queue=broker_point.queue,
            routing_key=broker_point.routing_key)
        consumer = Consumer(
            broker=self, id_=consumer_id,
            connection=self._connection,
            exchange=broker_point.exchange,
            exchange_type=broker_point.exchange_type,
            queue_=broker_point.queue,
            routing_key=broker_point.routing_key)

        consumer.add_task(task_id)
        self._consumers[consumer_id] = consumer

    def task_send(self, request):
        if request.scheduler:
            consumer_id = self._retry_consumer_name if request.scheduler.countdown else self._eta_consumer_name
        else:
            consumer_id = self._consumer_id_format.format(
                exchange=request.broker_point.exchange,
                exchange_type=request.broker_point.exchange_type,
                queue=request.broker_point.queue,
                routing_key=request.broker_point.routing_key)
        self._consumers[consumer_id].publish(request=request)

    def task_done(self, broker_point, delivery_tag):
        consumer_id = self._consumer_id_format.format(
            exchange=broker_point.exchange,
            exchange_type=broker_point.exchange_type,
            queue=broker_point.queue,
            routing_key=broker_point.routing_key)
        self._consumers[consumer_id].acknowledge_message(delivery_tag=delivery_tag)

    def task_reject(self, broker_point, delivery_tag):
        consumer_id = self._consumer_id_format.format(
            exchange=broker_point.exchange,
            exchange_type=broker_point.exchange_type,
            queue=broker_point.queue,
            routing_key=broker_point.routing_key)
        self._consumers[consumer_id].reject_message(delivery_tag=delivery_tag)

    def on_pool_size_changed(self):
        for cname, consumer in self._consumers.items():
            new_prefetch_count = 0
            for tname, task in self.app.registry.items():
                consumer_id = self._consumer_id_format.format(
                    exchange=task.broker_point.exchange,
                    exchange_type=task.broker_point.exchange_type,
                    queue=task.broker_point.queue,
                    routing_key=task.broker_point.routing_key)
                if consumer_id == cname:
                    new_prefetch_count += len(task.pool.workers)
            if consumer.prefetch_count != new_prefetch_count:
                self.logger.info('prefetch count changed from %s to %s' % (consumer.prefetch_count, new_prefetch_count))
                try:
                    consumer.change_prefetch_count(new_prefetch_count)
                except pika.exceptions.ChannelClosed:
                    consumer.open_channel()
                    consumer.change_prefetch_count(new_prefetch_count)
                if consumer.prefetch_count == 0:
                    consumer.stop_consuming()

    def _connect(self):
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
            self._connection = self._connect()

            # There is now a new connection, needs a new ioloop to run
            self._connection.ioloop.start()

    def _close_connection(self):
        """This method closes the connection to RabbitMQ.

        """
        self.logger.debug('Closing connection')
        self._connection.close()


class Consumer:
    def __init__(self, broker, id_, connection, exchange, exchange_type, queue_, routing_key):
        self.logger = logging.getLogger('gromozeka.broker.consumer')
        self.broker = broker
        self.prefetch_count = 0
        self.exchange = exchange
        self.exchange_type = exchange_type
        self.queue = queue_
        self.routing_key = routing_key
        self.id = id_
        self._channel = None
        self._connection = connection
        self._closing = False
        self._consumer_tag = None
        self.tasks = {}

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
        self.tasks[task_id] = self.broker.app.get_task(task_id)

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
        request = Request.from_json(json_message=body, delivery_tag=basic_deliver.delivery_tag)
        if self.queue in (self.broker._retry_queue, self.broker._eta_queue):
            if request.scheduler.every and not request.scheduler.eta:  # raw task
                delay = scheduler.delay(last_run=request.scheduler.eta,
                                        every=request.scheduler.every,
                                        interval=request.scheduler.interval,
                                        at=request.scheduler.at)
            else:
                delay = scheduler.delay_from_eta(
                    eta=request.scheduler.countdown if self.queue == self.broker._retry_queue else request.scheduler.eta)
            if delay > 0:
                self.broker.app.scheduler.add(
                    wait_time=delay,
                    func=self.publish,
                    args=[request, request.broker_point.exchange, request.broker_point.routing_key],
                    priority=1 if self.queue == self.broker._retry_queue else 2)
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
