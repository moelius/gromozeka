Running tasks from outer application
------------------------------------

We can run tasks from outer application or even other language, using task protocol. For now it's simply json.
Later other serializers will be available.

Define function:

.. literalinclude:: ../../../examples/raw_task.py
    :lines: 9-18

Start application:

.. literalinclude:: ../../../examples/raw_task.py
    :lines: 22-30
    :dedent: 4

Then make protobuf message:

.. literalinclude:: ../../../examples/raw_task.py
    :lines: 32-
    :dedent: 4

You can use custom proto files with custom deserealization:

.. literalinclude:: ../../../examples/raw_task_custom_proto.py
    :lines: 35-
    :dedent: 4

See `primitives.task.proto` for more information

