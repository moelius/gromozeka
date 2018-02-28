=========
Gromozeka
=========

Gromozeka is distributed task queue, based on RabbitMQ.

Features
--------

- RabbitMQ as broker
- simple definition of a task as a normal function.
- ability to retry on error (max_retries and retry_countdown options).
- ability to bind task as self option to worker function.
- ability to make control on workers with internal API

TODO's
------
- [ ] Tests
- [ ] Workflows (need's result backend)

Installation
------------

As usually use pip:

.. code-block:: bash

    pip install gromozeka

Documentation
-------------

See full `documentation <http://gromozeka.readthedocs.io/en/latest/>`_