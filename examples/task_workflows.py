from functools import reduce

from gromozeka import BrokerPoint, task
from gromozeka import Gromozeka, ThreadWorker


@task(bind=True)
def workflow(self):
    """

    Args:
        self(gromozeka.primitives.Task):

    Returns:

    """
    (((print_something('hello') >
       ((reduce(lambda x, y: x | y, [get_something_from_api(name) for name in ('boo', 'gaa', 'goo')])) >>
        print_something()))) >
     print_something('good by')).apply_async()


@task(bind=True)
def print_something(self, word):
    """

    Args:
        self(gromozeka.primitives.Task):
        word(str): Word to print
     """
    self.logger.info(word)


@task(bind=True)
def get_something_from_api(self, api_name):
    return api_name


if __name__ == '__main__':
    app = Gromozeka().config_from_env()
    workflow_broker_point = BrokerPoint(exchange='workflow', exchange_type='direct', queue='workflow',
                                        routing_key='workflow')
    broker_point_first = BrokerPoint(exchange='print', exchange_type='direct', queue='print',
                                     routing_key='print_something')
    broker_point_api = BrokerPoint(exchange='api', exchange_type='direct',
                                   queue='api', routing_key='get_something_from_api')

    # You can mix worker types (ThreadWorker,ProcessWorker)
    # `ThreadWorker` is good on IO operations
    # `ProcessWorker` when you have long CPU operations
    app.register(task=workflow(), broker_point=workflow_broker_point, worker_class=ThreadWorker, max_workers=1)
    app.register(task=print_something(), broker_point=broker_point_first, worker_class=ThreadWorker, max_workers=10)
    app.register(task=get_something_from_api(), broker_point=broker_point_api, worker_class=ThreadWorker,
                 max_workers=10)

    # Start application
    app.start()

    # Run tasks
    for i in range(1):
        workflow().apply_async()
