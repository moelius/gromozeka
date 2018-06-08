import time

# first example function
from gromozeka import Gromozeka, ThreadWorker, ProcessWorker
from gromozeka import BrokerPoint, task


@task(bind=True)
def test_func_one(self, sleep_time, word):
    """

    Args:
        self(gromozeka.primitives.Task):
        sleep_time(int): Time to sleep
        word(str): Word to print
     """
    self.logger.info('start working')
    time.sleep(sleep_time)
    self.logger.info('Job is done. Word is: %s' % word)


# second example function
@task(bind=True)
def test_func_second(self, sleep_time, word):
    """

    Args:
        self(gromozeka.primitives.Task):
        sleep_time(int): Time to sleep
        word(str): Word to print
    """
    self.logger.info('start working')
    time.sleep(sleep_time)
    self.logger.info('Job is done. Word is: %s' % word)


if __name__ == '__main__':
    app = Gromozeka()

    broker_point_first = BrokerPoint(exchange='first_exchange', exchange_type='direct', queue='first_queue',
                                     routing_key='first')
    broker_point_second = BrokerPoint(exchange='second_exchange', exchange_type='direct', queue='second_queue',
                                      routing_key='second')

    # You can mix worker types (ThreadWorker,ProcessWorker)
    # `ThreadWorker` is good on IO operations
    # `ProcessWorker` when you have long CPU operations
    app.register(task=test_func_one(), broker_point=broker_point_first, worker_class=ThreadWorker, max_workers=10)
    app.register(task=test_func_second(), broker_point=broker_point_second, worker_class=ProcessWorker,
                 max_workers=10)

    # Start application
    app.start()

    # Run tasks
    for i in range(10):
        test_func_one().apply_async(sleep_time=5, word="{}".format(ThreadWorker.__name__))
        test_func_second().apply_async(sleep_time=5, word="{}".format(ProcessWorker.__name__))
