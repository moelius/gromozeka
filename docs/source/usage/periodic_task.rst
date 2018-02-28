Making periodic and ETA tasks
-----------------------------

We can run task periodically

.. literalinclude:: ../../../examples/periodic_task.py

We can use 'seconds','minutes','hours','days','weeks' as period.

If you need to run task at some time, for example every week, use `at` argument:

.. code-block:: python

    @task(bind=True, every='weeks', interval=1,at='15:40')

Alternatively, we can run the task this way

.. code-block:: python

    test_func_one().every('seconds').interval(5).apply_async(word="hello")

We can run ETA task once:

.. code-block:: python

    test_func_one().eta('2018-02-28 14:19:00').apply_async(word="hello")

or when with task decorator:

.. code-block:: python

    @task(bind=True, eta='2018-02-28 14:19:00')

