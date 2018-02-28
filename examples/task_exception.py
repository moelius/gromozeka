from gromozeka import task, Gromozeka, Retry, BrokerPointType


# task with exception
@task(bind=True, max_retries=10, retry_countdown=5)
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
    app = Gromozeka()
    broker_point = BrokerPointType(exchange='first_exchange', exchange_type='direct', queue='first_queue',
                                   routing_key='first')

    app.register_task(task=test_func_bad(), broker_point=broker_point)
    # Start application
    app.start()
    test_func_bad().apply_async()
