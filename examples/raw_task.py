import json

import pika

from gromozeka import Gromozeka, task, BrokerPoint
from gromozeka import ProtoTask


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

    broker_point = BrokerPoint(exchange='first_exchange', exchange_type='direct', queue='first_queue',
                               routing_key='first')

    app.register(task=test_func_one(), broker_point=broker_point)

    # Start application
    app.start()

    # You can apply task from other application or other programming language, using RabbitMQ. You must copy task.proto
    # from gromozeka.promitives directory
    message = ProtoTask(task_id='__main__.test_func_one', kwargs=json.dumps({'word': 'hello'}))
    # You can apply task with custom uuid
    # message = Task(uuid='custom task_uuid', task_id='__main__.test_func_one',
    #                kwargs=json.dumps({'word': 'hello'}),
    #                reply_to=ProtoReplyToBrokerPoint(exchange='first_exchange', routing_key='first'))
    # If you do not want to copy task.proto for some reasons you can use custom proto file and use deserializator.
    # For example we have examples/custom_task.proto for example (You can find it in examples directory):

    connection = pika.BlockingConnection(pika.URLParameters('amqp://guest:guest@localhost:5672/%2F'))
    channel = connection.channel()
    channel.basic_publish(exchange='first_exchange', routing_key='first', body=message.SerializeToString())
    connection.close()
