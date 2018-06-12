=========
Gromozeka
=========

Gromozeka is distributed task queue, based on RabbitMQ.

.. image:: https://readthedocs.org/projects/gromozeka/badge/?version=latest
    :target: https://gromozeka.readthedocs.io/en/latest/?badge=latest
    :alt: Documentation Status

Features
--------

- Brokers:

 - RabbitMQ

- Result backends:

 - Redis

- simple definition of a task as a normal function.
- ability to retry on error (max_retries and retry_countdown options).
- ability to bind task as self option to worker function.
- ability to make control on workers with internal API

TODO's
------
- [ ] Tests
- [x] Workflows (need's result backend_adapter)

Installation
------------

As usually use pip:

.. code-block:: bash

    pip install gromozeka

Documentation
-------------

See full `documentation <http://gromozeka.readthedocs.io/en/latest/>`_