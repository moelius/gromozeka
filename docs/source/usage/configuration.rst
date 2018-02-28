Configuration
-------------

Available options:

================================ =====================================  =======================================
OPTION                           DEFAULT                                WHAT IS IT
================================ =====================================  =======================================
debug                            False                                  Logger level
broker_reconnect_max_retries     10                                     Broker reconnect attempts
broker_reconnect_retry_countdown 10                                     Sleep time between reconnects (seconds)
broker_retry_exchange            countdown_exchange                     Broker exchange for retries
broker_retry_queue               countdown_queue                        Broker queue name for retries
broker_retry_routing_key         countdown                              Broker routing key for retries
broker_eta_exchange              eta_exchange                           Broker exchange for eta tasks
broker_eta_queue                 eta_queue                              Broker queue for eta tasks
broker_eta_routing_key           eta                                    Broker routing key for eta tasks
broker_url                       amqp://guest:guest@localhost:5672/%2F  Broker url
================================ =====================================  =======================================

You can configure application from environment variables or with dict object:

* Environment

.. code-block:: python

    app=Gromozeka().config_from_env()

* With dictionary:

.. code-block:: python

    conf={'app_prefix':'my_application','broker_reconnect_max_retries':3}
    app=Gromozeka().config_from_dict(conf)