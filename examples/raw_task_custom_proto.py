import json

import pika

from gromozeka import Gromozeka, task, BrokerPoint, TaskDeserializator
from examples.custom_task_pb2 import Word, Task


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
    # You can use custom proto file and use TaskDeserializator.
    # For example we have examples/custom_task.proto for example (You can find it in examples directory):
    # syntax = "proto3";
    #
    # package custom_task;
    #
    # message Word {
    #     string word = 1;
    # }
    #
    # message Task {
    #     string task_id = 2;
    #     Word word = 3;
    # }

    class CustomDeserializator(TaskDeserializator):
        def deserialize(self, raw_task):
            t = Task()
            t.ParseFromString(raw_task)
            # You must return task_uuid, task_id, graph_uuid, args, kwargs, retries, delay, reply_to_exchange,reply_to_routing_key
            # Required fields are: task_id.
            return None, t.task_id, None, [t.word.word], None, None, None, None, None  # args - list, kwargs - dict


    message = Task(task_id='__main__.test_func_one', word=Word(word='hello'))
    app = Gromozeka()
    broker_point = BrokerPoint(exchange='first_exchange', exchange_type='direct', queue='first_queue',
                               routing_key='first')

    app.register(task=test_func_one(), broker_point=broker_point, deserializator=CustomDeserializator())

    # Start application
    app.start()

    connection = pika.BlockingConnection(pika.URLParameters('amqp://guest:guest@localhost:5672/%2F'))
    channel = connection.channel()
    channel.basic_publish(exchange='first_exchange', routing_key='first', body=message.SerializeToString())
    connection.close()
