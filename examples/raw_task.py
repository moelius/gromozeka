import json
import uuid

import pika

from gromozeka import Gromozeka, BrokerPointType, task


# first example function
@task(bind=True)
def test_func_one(self, word):
    """

    Args:
        self(gromozeka.primitives.Task):
        word(str): Word to print
     """
    self.logger.info('Job is done. Word is: %s' % word)


if __name__ == '__main__':
    app = Gromozeka()

    broker_point = BrokerPointType(exchange='first_exchange', exchange_type='direct', queue='first_queue',
                                   routing_key='first')

    app.register_task(task=test_func_one(), broker_point=broker_point)

    # Start application
    app.start()

    # You can apply task from other application or other programming language, using RabbitMQ
    json_message = json.dumps(
        [  # task
            [
                uuid.uuid4().__str__(),  # task unique identification
                '__main__.test_func_one',  # task identification
                [],  # list of task function arguments
                {"word": "hello"},  # task function keyword arguments
            ],
        ])

    connection = pika.BlockingConnection(pika.URLParameters('amqp://guest:guest@localhost:5672/%2F'))
    channel = connection.channel()
    channel.basic_publish(exchange='first_exchange', routing_key='first', body=json_message)
    connection.close()
