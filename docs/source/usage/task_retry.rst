How to retry task
-----------------

To retry task on exception or when some condition occurred, we can use ``raise Retry(e)``:

.. literalinclude:: ../../../examples/task_exception.py
    :lines: 4-16

We can redefine behavior in task:

.. code-block:: python

    raise Retry(e,max_retries=3,retry_countdown=1)

Or when we register task:

.. code-block:: python

    app.register_task(task=test_func_bad(), broker_point=broker_point, max_workers=10, max_retries=5, retry_countdown=1)