import time

from gromozeka import task, Gromozeka, Retry, BrokerPoint


# task with exception
@task(bind=True, max_retries=3, retry_countdown=5)
def test_func_bad(self):
    """

    Args:
        self(gromozeka.primitives.Task):
    """
    self.logger.info('start working')
    try:
        1 / 0
    except ZeroDivisionError as e:
        raise Retry(e)


if __name__ == '__main__':
    app = Gromozeka().config_from_env()
    broker_point = BrokerPoint(exchange='first_exchange', exchange_type='direct', queue='first_queue',
                                    routing_key='first')

    app.register(task=test_func_bad(), broker_point=broker_point)
    # Start application
    app.start()
    time.sleep(2)
    test_func_bad().apply_async()
