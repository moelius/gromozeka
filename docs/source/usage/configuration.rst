Configuration
-------------

Available options:

=================================  =====================================  =======================================
OPTION                             DEFAULT                                WHAT IS IT
=================================  =====================================  =======================================
debug                              False                                  Logger level debug turn on
broker_reconnect_max_retries       10                                     Broker reconnect attempts
broker_reconnect_retry_countdown   10                                     Sleep time between reconnects (seconds)
broker_url                         amqp://guest:guest@localhost:5672/%2F  Broker url
backend_url                        redis://localhost                      Backend url
backend_reconnect_max_retries      10                                     Backend reconnect attempts
backend_reconnect_retry_countdown  10                                     Sleep time between reconnects (seconds)
=================================  =====================================  =======================================

You can configure application from environment variables or with dict object:

* Environment

.. code-block:: python

    app=Gromozeka().config_from_env()

* With dictionary:

.. code-block:: python

    conf={'app_prefix':'my_application','broker_reconnect_max_retries':3}
    app=Gromozeka().config_from_dict(conf)