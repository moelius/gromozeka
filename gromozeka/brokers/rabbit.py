import logging
import signal
import threading
import time

import pika
from pika import exceptions

import gromozeka.app
from gromozeka.brokers.base import BrokerInterface
from gromozeka.primitives import Task

APP_ID = 'gromozeka'
CONSUMER_ID_FORMAT = "{exchange}.{exchange_type}.{queue}.{routing_key}"
PIKA_CONNECTION_EXCEPTIONS = (exceptions.AMQPConnectionError,
                              exceptions.ConnectionClosed)
PIKA_CHANNEL_EXCEPTIONS = (
    exceptions.AMQPConnectionError,
    exceptions.ConnectionClosed,
    exceptions.ChannelClosed,
    OverflowError,
    AttributeError)


def format_consumer_id_from_broker_point(broker_point):
    return CONSUMER_ID_FORMAT.format(exchange=broker_point.exchange, exchange_type=broker_point.exchange_type,
                                     queue=broker_point.queue, routing_key=broker_point.routing_key)


class RabbitMQPikaAdaptee(BrokerInterface):
    """Pika adapted broker

    Attributes:
        reconnect_max_retries(int): Maximum attempts to reconnect
        reconnect_retry_countdown(int|float): Maximum attempts to reconnect
        connection(pika.SelectConnection): Pika connection

    """

    def __init__(self, app):
        """

        Args:
            app (gromozeka.app.app.Gromozeka):
        """
        self.reconnect_max_retries = None
        self.reconnect_retry_countdown = None
        self.connection = None
        self._consumers = {}
        self._url = None
        self._closing = False
        self.reconnecting = False
        self._prefetch_lock = threading.Lock()
        super().__init__(app=app)

    @staticmethod
    def worker_run(self):
        """

        Args:
            self(gromozeka.brokers.base.BrokerAdapter):

        Returns:

        """
        self.broker.connection = self.broker.connect()
        self.broker.connection.ioloop.start()

    def stop(self):
        self.stop_serve()

    def configure(self):
        self._url = self.app.config.broker_url
        self.reconnect_max_retries = self.app.config.broker_reconnect_max_retries
        self.reconnect_retry_countdown = self.app.config.broker_reconnect_retry_countdown

    def get_consumer(self, broker_point):
        return self._consumers[format_consumer_id_from_broker_point(broker_point)]

    def task_register(self, broker_point, task_id, options, deserializator):
        consumer_id = format_consumer_id_from_broker_point(broker_point)
        try:
            consumer = self._consumers[consumer_id]
        except KeyError:
            consumer = Consumer(
                exchange=broker_point.exchange,
                exchange_type=broker_point.exchange_type,
                queue=broker_point.queue,
                routing_key=broker_point.routing_key,
                deserializator=deserializator)
            self._consumers[consumer_id] = consumer
        consumer.add_task(task_id)

    def task_send(self, request, broker_point, reply_to=None):
        consumer = self._consumers[format_consumer_id_from_broker_point(broker_point)]
        consumer.publish(request, reply_to)

    def task_send_delayed(self, request, broker_point, delay):
        publisher = DelayedMessagePublisher(broker_connection=self.connection, request=request,
                                            original_exchange=broker_point.exchange,
                                            original_routing_key=broker_point.routing_key, delay=delay)
        publisher.publish()

    def task_done(self, task_uuid, broker_point, delivery_tag):
        consumer = self._consumers[format_consumer_id_from_broker_point(broker_point)]
        consumer.acknowledge_message(task_uuid, delivery_tag)

    def task_reject(self, broker_point, delivery_tag):
        consumer = self._consumers[format_consumer_id_from_broker_point(broker_point)]
        consumer.reject_message(delivery_tag)

    def on_pool_size_changed(self):
        with self._prefetch_lock:
            for cname, consumer in self._consumers.items():
                new_prefetch_count = 0
                for _, task in self.app.registry.items():
                    consumer_id = format_consumer_id_from_broker_point(task.broker_point)
                    if consumer_id == cname:
                        new_prefetch_count += task.pool.size
                if consumer.prefetch_count != new_prefetch_count:
                    consumer.change_prefetch_count(new_prefetch_count)
                    if consumer.prefetch_count == 0:
                        consumer.stop_consuming()

    def wait_for_start(self):
        wfs_consumers = [c for _, c in self._consumers.items()]
        while wfs_consumers:
            for n, c in enumerate(wfs_consumers):
                if c.is_consuming:
                    del (wfs_consumers[n])

    def connect(self):
        """This method connects to RabbitMQ, returning the connection handle.
        When the connection is established, the on_connection_open method
        will be invoked by pika.

        Returns:
            pika.SelectConnection:

        """
        self.logger.info('connecting to %s', self._url)
        return pika.adapters.SelectConnection(parameters=pika.URLParameters(self._url),
                                              on_open_callback=self.on_connection_open,
                                              stop_ioloop_on_close=False)

    def on_connection_open(self, unused_connection):
        """This method is called by pika once the connection to RabbitMQ has
        been established. It passes the handle to the connection object in
        case we need it, but in this case, we'll just mark it unused.

        Args:
            unused_connection(pika.SelectConnection): Unused connection

        """
        self.logger.info('connection opened')
        self.add_on_connection_close_callback()
        for _, consumer in self._consumers.items():
            consumer.connection = self.connection
            consumer.open_channel()

    def add_on_connection_close_callback(self):
        """This method adds an on close callback that will be invoked by pika
        when RabbitMQ closes the connection to the publisher unexpectedly.

        """
        self.logger.debug('adding connection close callback')
        self.connection.add_on_close_callback(self.on_connection_closed)

    def on_connection_closed(self, connection, reply_code, reply_text):
        """This method is invoked by pika when the connection to RabbitMQ is
        closed unexpectedly. Since it is unexpected, we will reconnect to
        RabbitMQ if it disconnects.

        Args:
            connection(pika.connection.Connection): The closed connection obj
            reply_code(int): Reply code if given
            reply_text(str): The server provided reply_text if given

        """
        for _, consumer in self._consumers.items():
            consumer._channel = None
        if self._closing:
            self.connection.ioloop.stop()
            self.logger.info('connection closed')
        else:
            if self.reconnect_max_retries:
                self.logger.warning('starting reconnection process in %d seconds: (%s) %s',
                                    self.reconnect_retry_countdown, reply_code, reply_text)
                self.connection.add_timeout(self.reconnect_retry_countdown, self.reconnect)
                return
            self.logger.fatal("broker connection problems: (%s) %s", reply_code, reply_text)
            self.stop()
            self.app.stop_signal(signal.SIGTERM)

    def reconnect(self):
        """Will be invoked by the IOLoop timer if the connection is
        closed. See the on_connection_closed method.

        """
        # This is the old connection IOLoop instance, stop its ioloop
        self.connection.ioloop.stop()
        if self._closing or self.connection.is_open:
            return
        attempts = self.reconnect_max_retries
        stop_exc = None
        while not self.app.is_closing:
            self.reconnecting = True
            # Create a new connection
            try:
                self.connection = self.connect()
                # There is now a new connection, needs a new ioloop to run
                self.connection.ioloop.start()
                self.reconnecting = False
                return
            except PIKA_CONNECTION_EXCEPTIONS as e:
                self.logger.warning(e)
                attempts -= 1
                if attempts <= 0:
                    stop_exc = "broker connection problems, max reconnects exceeded: %s" % e
                    break
                time.sleep(self.reconnect_retry_countdown)
                continue
            except Exception as e:
                stop_exc = "unhandled exception while reconnecting: %s" % e
                break
        if self.app.is_closing:
            return
        self.logger.fatal(stop_exc)
        self.stop()
        self.app.stop_signal(signal.SIGTERM)

    def stop_serve(self):
        """Cleanly shutdown the connection to RabbitMQ by stopping the consumers
        with RabbitMQ. When RabbitMQ confirms the cancellation, on_cancelok
        will be invoked by pika, which will then closing the channel and
        connection. The IOLoop is started again because this method is invoked
        when CTRL-C is pressed raising a KeyboardInterrupt exception. This
        exception stops the IOLoop which needs to be running for pika to
        communicate with RabbitMQ. All of the commands issued prior to starting
        the IOLoop will be buffered but not processed.

        """
        self._closing = True
        wfs_consumers = []
        for _, consumer in self._consumers.items():
            if not consumer.channel or consumer.channel.is_closed or consumer.channel.is_closing:
                continue
            consumer.stop_consuming()
            wfs_consumers.append(consumer)
        while wfs_consumers:
            for n, c in enumerate(wfs_consumers):
                if c.channel.is_closed:
                    del (wfs_consumers[n])
        if self.connection.is_open:
            self.close_connection()
        self.connection.ioloop.stop()

    def close_connection(self):
        """This method closes the connection to RabbitMQ."""
        self.logger.info('closing connection')
        self.connection.close()


class DelayedMessagePublisher:
    exchange = 'delayed'
    exchange_type = 'direct'

    def __init__(self, broker_connection, request, original_exchange, original_routing_key, delay):
        self.logger = logging.getLogger('gromozeka.broker.consumer.delayed_publisher')
        self._connection = broker_connection
        self.ttl = delay * 1000
        self.queue = 'delay.%d.%s.%s' % (self.ttl, original_exchange, original_routing_key)
        self.routing_key = self.queue
        self.original_exchange = original_exchange
        self.original_routing_key = original_routing_key
        self.request = request
        self.channel = None

    def open_channel(self):
        """Open a new channel with RabbitMQ by issuing the Channel.Open RPC
        command. When RabbitMQ responds that the channel is open, the
        on_channel_open callback will be invoked by pika.

        """
        self.logger.debug('creating a new channel')
        self._connection.channel(on_open_callback=self.on_channel_open)

    def on_channel_open(self, channel):
        """This method is invoked by pika when the channel has been opened.
        The channel object is passed in so we can make use of it.

        Since the channel is now open, we'll declare the exchange to use.

        Args:
            channel(pika.channel.Channel): The closed channel

        """
        self.logger.debug('channel opened')
        self.channel = channel
        self.add_on_channel_close_callback()
        self.setup_exchange(self.exchange)

    def add_on_channel_close_callback(self):
        """This method tells pika to call the on_channel_closed method if
        RabbitMQ unexpectedly closes the channel.

        """
        self.logger.debug('adding channel close callback')
        self.channel.add_on_close_callback(self.on_channel_closed)

    def on_channel_closed(self, channel, reply_code, reply_text):
        """Invoked by pika when RabbitMQ unexpectedly closes the channel.
        Channels are usually closed if you attempt to do something that
        violates the protocol, such as re-declare an exchange or queue with
        different parameters. In this case, we'll close the connection
        to shutdown the object.

        Args:
            channel(pika.channel.Channel): The closed channel
            reply_code(int): The numeric reason the channel was closed
            reply_text(str): The text reason the channel was closed
        """
        self.logger.debug('channel %i was closed: (%s) %s', channel, reply_code, reply_text)

    def setup_exchange(self, exchange_name):
        """Setup the exchange on RabbitMQ by invoking the Exchange.Declare RPC
        command. When it is complete, the on_exchange_declareok method will
        be invoked by pika.

        Args:
            exchange_name(str): The name of the exchange to declare

        """
        self.logger.debug('declaring exchange %s', exchange_name)
        self.channel.exchange_declare(self.on_exchange_declareok, exchange_name, self.exchange_type)

    def on_exchange_declareok(self, unused_frame):
        """Invoked by pika when RabbitMQ has finished the Exchange.Declare RPC
        command.

        :param pika.Frame.Method unused_frame: Exchange.DeclareOk response frame

        """
        self.logger.debug('exchange declared')
        self.setup_queue(self.queue)

    def setup_queue(self, queue_name, ):
        """Setup the queue on RabbitMQ by invoking the Queue.Declare RPC
        command. When it is complete, the on_queue_declareok method will
        be invoked by pika.

        :param str|unicode queue_name: The name of the queue to declare.

        """
        self.logger.debug('declaring queue %s', queue_name)
        self.channel.queue_declare(self.on_queue_declareok, queue=queue_name, durable=True, arguments={
            'x-dead-letter-exchange': self.original_exchange,  # Exchange used to transfer the message from A to B.
            'x-dead-letter-routing-key': self.original_routing_key,  # Name of the queue message transferred to.
            'x-message-ttl': self.ttl,  # Delay until the message is transferred in milliseconds.
            'x-expires': self.ttl * 2
        })

    def on_queue_declareok(self, method_frame):
        """Method invoked by pika when the Queue.Declare RPC call made in
        setup_queue has completed. In this method we will bind the queue
        and exchange together with the routing key by issuing the Queue.Bind
        RPC command. When this command is complete, the on_bindok method will
        be invoked by pika.

        Args:
            method_frame(pika.frame.Method): The Queue.DeclareOk frame

        """
        self.logger.debug('binding %s to %s with %s', self.exchange, self.queue, self.routing_key)
        self.channel.queue_bind(self.on_bindok, self.queue, self.exchange, self.routing_key)

    def on_bindok(self, unused_frame):
        """Invoked by pika when the Queue.Bind method has completed. At this
        point we will start consuming messages by calling start_consuming
        which will invoke the needed RPC commands to start the process.

        Args:
            unused_frame(pika.frame.Method): The Queue.BindOk response frame

        """
        self.logger.debug('queue bound')
        try:
            self.channel.basic_publish(self.exchange, self.queue, self.request,
                                       properties=pika.BasicProperties(delivery_mode=2))
        except PIKA_CHANNEL_EXCEPTIONS:
            self.logger.warning('connection lost on message retry, retry will be ignored')
            return
        self.close_channel()

    def add_on_cancel_callback(self):
        """Add a callback that will be invoked if RabbitMQ cancels the consumer
        for some reason. If RabbitMQ does cancel the consumer,
        on_consumer_cancelled will be invoked by pika.

        """
        self.logger.debug('adding consumer cancellation callback')
        self.channel.add_on_cancel_callback(self.on_consumer_cancelled)

    def on_consumer_cancelled(self, method_frame):
        """Invoked by pika when RabbitMQ sends a Basic.Cancel for a consumer
        receiving messages.

        Args:
            method_frame(pika.frame.Method): The Basic.Cancel frame

        """
        self.logger.info('consumer was cancelled remotely, shutting down: %r', method_frame)
        if self.channel:
            self.channel.close()

    def on_cancelok(self, unused_frame):
        """This method is invoked by pika when RabbitMQ acknowledges the
        cancellation of a consumer. At this point we will close the channel.
        This will invoke the on_channel_closed method once the channel has been
        closed, which will in-turn close the connection.

        Args:
            unused_frame(pika.frame.Method): The Basic.CancelOk frame

        """
        self.logger.debug('RabbitMQ acknowledged the cancellation of the consumer')
        self.close_channel()

    def close_channel(self):
        """Call to close the channel with RabbitMQ cleanly by issuing the
        Channel.Close RPC command.

        """
        self.logger.debug('closing the channel')
        self.channel.close()

    def publish(self):
        self.open_channel()


class Consumer:
    def __init__(self, exchange, exchange_type, queue, routing_key, deserializator=None):
        consumer_id = "%s.%s.%s.%s" % (exchange, exchange_type, queue, routing_key)
        self.logger = logging.getLogger('gromozeka.broker.consumer.%s' % consumer_id)
        self.consumer_tag = consumer_id
        self.exchange = exchange
        self.exchange_type = exchange_type
        self.queue = queue
        self.routing_key = routing_key
        self.tasks = {}
        self.channel = None
        self.connection = None
        self._closing = False
        self.is_consuming = False
        self.prefetch_count = 0
        self.stop_lock = threading.Lock()
        self.deserializator = deserializator
        from gromozeka.brokers import LatexSema
        self.sema = LatexSema(threading.Semaphore, self.prefetch_count)

    def add_task(self, task_id):
        self.tasks[task_id] = gromozeka.app.get_app().get_task(task_id)

    def open_channel(self):
        """Open a new channel with RabbitMQ by issuing the Channel.Open RPC
        command. When RabbitMQ responds that the channel is open, the
        on_channel_open callback will be invoked by pika.

        """
        self.logger.debug('creating a new channel')
        self.connection.channel(on_open_callback=self.on_channel_open)

    def on_channel_open(self, channel):
        """This method is invoked by pika when the channel has been opened.
        The channel object is passed in so we can make use of it.

        Since the channel is now open, we'll declare the exchange to use.

        Args:
            channel(pika.channel.Channel): The closed channel

        """
        self.logger.debug('channel opened')
        self.channel = channel
        self.add_on_channel_close_callback()
        self.setup_exchange(self.exchange)

    def add_on_channel_close_callback(self):
        """This method tells pika to call the on_channel_closed method if
        RabbitMQ unexpectedly closes the channel.

        """
        self.logger.debug('adding channel close callback')
        self.channel.add_on_close_callback(self.on_channel_closed)

    def on_channel_closed(self, channel, reply_code, reply_text):
        """Invoked by pika when RabbitMQ unexpectedly closes the channel.
        Channels are usually closed if you attempt to do something that
        violates the protocol, such as re-declare an exchange or queue with
        different parameters. In this case, we'll close the connection
        to shutdown the object.

        Args:
            channel(pika.channel.Channel): The closed channel
            reply_code(int): The numeric reason the channel was closed
            reply_text(str): The text reason the channel was closed
        """
        self.logger.debug('channel %i was closed: (%s) %s', channel, reply_code, reply_text)

    def setup_exchange(self, exchange_name):
        """Setup the exchange on RabbitMQ by invoking the Exchange.Declare RPC
        command. When it is complete, the on_exchange_declareok method will
        be invoked by pika.

        Args:
            exchange_name(str): The name of the exchange to declare

        """
        self.logger.debug('declaring exchange %s', exchange_name)
        self.channel.exchange_declare(self.on_exchange_declareok, exchange_name, self.exchange_type)

    def on_exchange_declareok(self, unused_frame):
        """Invoked by pika when RabbitMQ has finished the Exchange.Declare RPC
        command.

        :param pika.Frame.Method unused_frame: Exchange.DeclareOk response frame

        """
        self.logger.debug('exchange declared')
        self.setup_queue(self.queue)

    def setup_queue(self, queue_name):
        """Setup the queue on RabbitMQ by invoking the Queue.Declare RPC
        command. When it is complete, the on_queue_declareok method will
        be invoked by pika.

        :param str|unicode queue_name: The name of the queue to declare.

        """
        self.logger.debug('declaring queue %s', queue_name)
        self.channel.queue_declare(self.on_queue_declareok, queue_name, durable=True)

    def on_queue_declareok(self, method_frame):
        """Method invoked by pika when the Queue.Declare RPC call made in
        setup_queue has completed. In this method we will bind the queue
        and exchange together with the routing key by issuing the Queue.Bind
        RPC command. When this command is complete, the on_bindok method will
        be invoked by pika.

        Args:
            method_frame(pika.frame.Method): The Queue.DeclareOk frame

        """
        self.logger.debug('binding %s to %s with %s', self.exchange, self.queue, self.routing_key)
        self.channel.queue_bind(self.on_bindok, self.queue, self.exchange, self.routing_key)

    def on_bindok(self, unused_frame):
        """Invoked by pika when the Queue.Bind method has completed. At this
        point we will start consuming messages by calling start_consuming
        which will invoke the needed RPC commands to start the process.

        Args:
            unused_frame(pika.frame.Method): The Queue.BindOk response frame

        """
        self.logger.debug('queue bound')
        self.start_consuming()

    def start_consuming(self):
        """This method sets up the consumer by first calling
        add_on_cancel_callback so that the object is notified if RabbitMQ
        cancels the consumer. It then issues the Basic.Consume RPC command
        which returns the consumer tag that is used to uniquely identify the
        consumer with RabbitMQ. We keep the value to use it when we want to
        cancel consuming. The on_message method is passed in as a callback pika
        will invoke when a message is fully received.

        """
        self.logger.debug('issuing consumer related RPC commands')
        self.channel.basic_qos(prefetch_count=self.prefetch_count)
        self.add_on_cancel_callback()
        self.consumer_tag = self.channel.basic_consume(self.on_message, self.queue)
        self.is_consuming = True

    def add_on_cancel_callback(self):
        """Add a callback that will be invoked if RabbitMQ cancels the consumer
        for some reason. If RabbitMQ does cancel the consumer,
        on_consumer_cancelled will be invoked by pika.

        """
        self.logger.debug('adding consumer cancellation callback')
        self.channel.add_on_cancel_callback(self.on_consumer_cancelled)

    def on_consumer_cancelled(self, method_frame):
        """Invoked by pika when RabbitMQ sends a Basic.Cancel for a consumer
        receiving messages.

        Args:
            method_frame(pika.frame.Method): The Basic.Cancel frame

        """
        self.logger.info('consumer was cancelled remotely, shutting down: %r', method_frame)
        if self.channel:
            self.channel.close()

    def change_prefetch_count(self, new_size):
        if self.channel and self.channel.is_open:
            self.channel.basic_qos(prefetch_size=new_size if self.prefetch_count < new_size else 0,
                                   prefetch_count=new_size)
        self.logger.info('prefetch count changed from %s to %s', self.prefetch_count, new_size)
        self.prefetch_count = new_size
        self.sema.change(new_size)

    def publish(self, request, exchange=None, routing_key=None):
        """

        Args:
            request:
            exchange(str):
            routing_key(str):

        Returns:

        """
        if exchange and routing_key:
            exchange = exchange
            routing_key = routing_key
        else:
            exchange = self.exchange
            routing_key = self.routing_key
        try:
            self.channel.basic_publish(exchange=exchange, routing_key=routing_key, body=request,
                                       properties=pika.BasicProperties(delivery_mode=2, app_id=APP_ID))
        except PIKA_CHANNEL_EXCEPTIONS:
            self.logger.warning('connection lost on message publish')

    def on_message(self, unused_channel, basic_deliver, properties, body):
        """Invoked by pika when a message is delivered from RabbitMQ. The
        channel is passed for your convenience. The basic_deliver object that
        is passed in carries the exchange, routing key, delivery tag and
        a redelivered flag for the message. The properties passed in is an
        instance of BasicProperties with the message properties and the body
        is the message that was sent.

        Args:
            unused_channel(pika.channel.Channel): The channel object
            basic_deliver(pika.Spec.Basic.Deliver): Basic deliver method
            properties(pika.Spec.BasicProperties): Properties
            body(str|bytes): The message body

        """
        self.sema.acquire()
        task = Task.from_proto(body, deserializator=self.deserializator if properties.app_id != APP_ID else None)

        self.logger.info('received message #%s, task_uuid <%s> from %s', basic_deliver.delivery_tag, task.uuid,
                         properties.app_id)
        task.delivery_tag = basic_deliver.delivery_tag
        task.on_receive()
        self.tasks[task.task_id].pool.worker_queue.put(task.request.SerializeToString())

    def acknowledge_message(self, task_uuid, delivery_tag):
        """Acknowledge the message delivery from RabbitMQ by sending a
        Basic.Ack RPC method for the delivery tag.

        Args:
            task_uuid:
            delivery_tag(int): The delivery tag from the Basic.Deliver frame

        """
        try:
            self.channel.basic_ack(delivery_tag)
        except PIKA_CHANNEL_EXCEPTIONS:
            self.logger.warning('connection lost on message acknowledge #%d' % delivery_tag)
        finally:
            self.sema.release()
        self.logger.info('acknowledged message #%d, task_uuid <%s>', delivery_tag, task_uuid)

    def reject_message(self, delivery_tag, requeue=False):
        """Reject message by it`s delivery tag

        Args:
            delivery_tag(int):
            requeue(bool): If `True` message will reject with requeue
        """
        self.logger.info('rejecting message %s', delivery_tag)
        try:
            self.channel.basic_reject(delivery_tag, requeue)
        except PIKA_CHANNEL_EXCEPTIONS:
            self.logger.warning('connection lost on message reject #%d' % delivery_tag)
        finally:
            self.sema.release()

    def stop_consuming(self):
        """Tell RabbitMQ that you would like to stop consuming by sending the
        Basic.Cancel RPC command.

        """
        self.stop_lock.acquire()
        if not self.channel:
            return
        if self.channel.is_closing or self.channel.is_closed:
            return
        self.logger.debug('sending a Basic.Cancel RPC command to RabbitMQ')
        self.channel.basic_cancel(self.on_cancelok, self.consumer_tag)

    def on_cancelok(self, unused_frame):
        """This method is invoked by pika when RabbitMQ acknowledges the
        cancellation of a consumer. At this point we will close the channel.
        This will invoke the on_channel_closed method once the channel has been
        closed, which will in-turn close the connection.

        Args:
            unused_frame(pika.frame.Method): The Basic.CancelOk frame

        """
        self.logger.debug('broker acknowledged the cancellation of the consumer')
        self.close_channel()
        self.stop_lock.release()

    def close_channel(self):
        """Call to close the channel with RabbitMQ cleanly by issuing the
        Channel.Close RPC command.

        """
        self.logger.debug('closing the channel')
        self.channel.close()
