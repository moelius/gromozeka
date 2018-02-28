Running tasks from outer application
------------------------------------

We can run tasks from outer application or even other language, using task protocol. For now it's simply json.
Later other serializers will be available.

.. literalinclude:: ../../../examples/raw_task.py
    :lines: 32-
    :dedent: 4

Periodic tasks:

.. literalinclude:: ../../../examples/raw_periodic_task.py
    :lines: 35-
    :dedent: 4

ETA tasks:

.. literalinclude:: ../../../examples/raw_eta_task.py
    :lines: 35-
    :dedent: 4



See `primitives.protocol` for more information