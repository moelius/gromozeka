from gromozeka import task, Gromozeka, BrokerPointType


# Example function
@task(bind=True, every='seconds', interval=5)
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

    # You must run this once
    test_func_one().apply_async(word="hello")
