Building task workflows
-----------------------

We can build unlimited workflows with only three operators:

- **CHAIN** **">" operator**. Chain of tasks. Tasks will be executed one by one.
- **ACHAIN** **">>" operator**. Argumented chain of tasks. Tasks will be executed one by one, and next one will have result from previous task as first argument.
- **GROUP** **"|" operator**. Parallel tasks execution.

Define functions:

.. literalinclude:: ../../../examples/task_workflows.py
    :lines: 7-36

First ``print_something('hello')`` will execute:

Then in workflow function we can see:

.. code-block:: python

    reduce(lambda x, y: x | y, [get_something_from_api(name) for name in ('boo', 'gaa', 'goo')])

This operation makes 3 tasks: ``get_something_from_api('boo'),get_something_from_api('gaa'),get_something_from_api('goo')``
and will execute them in parallel and collect results in a group ['boo', 'gaa', 'goo']

Then ``print_something()`` will be executed with argument from previous group result:
and output from this function will be: ``['boo', 'gaa', 'goo']``

And last operation will be: ``print_something('good by')``



