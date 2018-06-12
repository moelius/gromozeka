Visualization of the workflow
-----------------------------


We can see graphically graph of tasks:

You must install gromozeka with visual bundle: ``pip install "gromozeka[visual]"``,
and graphviz must be installed on your system (https://graphviz.gitlab.io/download/).

For example we want to see graph from this workflow: :doc:`../usage/task_workflows`

.. code-block:: python

    (((print_something('hello') >
       ((reduce(lambda x, y: x | y, [get_something_from_api(name) for name in ('boo', 'gaa', 'goo')])) >>
        print_something()))) >
     print_something('good by')).draw()

