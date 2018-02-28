import json
import uuid

import pika

from gromozeka import Gromozeka, BrokerPointType, ThreadWorker, task


# first example function
@task(bind=True)
def test_func_one(self, word):
    """

    Args:
        self(gromozeka.primitives.Task):
        word(str): Word to print

    Returns:

    """
    self.logger.info('Job is done. Word is: %s' % word)


if __name__ == '__main__':
    app = Gromozeka()

    broker_point = BrokerPointType(exchange='first_exchange', exchange_type='direct', queue='first_queue',
                                   routing_key='first')

    app.register_task(task=test_func_one(), broker_point=broker_point)

    # Start application
    app.start()

    # Periodic task using protocol see `primitives.protocol`. This task will run every day at 15:04:00
    json_message = json.dumps(
        [
            # task
            [
                uuid.uuid4().__str__(),  # task unique identification
                '__main__.test_func_one',  # task function path
                [],  # list of task function arguments
                {"word": "hello"},  # task function keyword arguments
            ],
            # Scheduler
            [
                None,  # eta - When task must run formatted as "%Y-%m-%d %H:%M:%S"
                'days',  # period
                '1',  # interval
                '15:04:00',  # Time
            ],
        ])

    connection = pika.BlockingConnection(pika.URLParameters('amqp://guest:guest@localhost:5672/%2F'))
    channel = connection.channel()
    # You must push periodic and eta tasks to eta exchange.
    channel.basic_publish(exchange='eta_exchange', routing_key='eta', body=json_message)
    connection.close()
