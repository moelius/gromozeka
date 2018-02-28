Making and running tasks
------------------------

Define tasks from functions, using decorator `@task`:

.. literalinclude:: ../../../examples/task.py
    :lines: 1-33

Define application:

.. literalinclude:: ../../../examples/task.py
    :lines: 37
    :dedent: 4

Define `RabbitMQ` queues:

.. literalinclude:: ../../../examples/task.py
    :lines: 39-42
    :dedent: 4

Register tasks in application:

.. literalinclude:: ../../../examples/task.py
    :lines: 44-49
    :dedent: 4

Or task can register self:

.. code-block:: python

    test_func_one().register(broker_point=broker_point_first, max_workers=10, worker_class=ThreadWorker)
    test_func_second().register(broker_point=broker_point_second, max_workers=10, worker_class=ProcessWorker)

You can start application now:

.. literalinclude:: ../../../examples/task.py
    :lines: 52
    :dedent: 4

Add 10 tasks for each queue:

.. literalinclude:: ../../../examples/task.py
    :lines: 54-
    :dedent: 4




