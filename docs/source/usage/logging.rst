How to configure logging
------------------------

Gromozeka have default loggers:

* gromozeka - root application logger
* gromozeka.pool - Pool logger
* gromozeka.pool.worker -  Workers logger
* gromozeka.scheduler - Scheduler logger
* gromozeka.broker - Broker logger
* gromozeka.broker.consumer - Consumer logger


If you want to reconfigure some logger:

.. code-block:: python

    # This will configure all existing loggers
    gromozeka_logger = logging.getLogger('gromozeka')
    gromozeka_logger.setLevel(logging.WARNING)

    # Special logger
    broker_logger = logging.getLogger('gromozeka.broker')
    broker_logger.setLevel(logging.DEBUG)

Or you can configure with dictionary. This must be run after application configure.

.. code-block:: python

    from logging import config

    LOGGING = {
        'version': 1,
        'disable_existing_loggers': True,
        'formatters': {
            'console': {
                'format': '%(asctime)-15s %(levelname)s %(name)s [%(threadName)s %(processName)s] %(message)s'
            },
        },
        'handlers': {
            'console': {
                'level': 'DEBUG',
                'class': 'logging.StreamHandler',
                'formatter': 'console'
            },
        },
        'loggers': {
            'gromozeka': {'handlers': ['console'], 'level': 'INFO'},
            'gromozeka.pool': {},
            'gromozeka.pool.worker': {},
            'gromozeka.pool.worker.*': {},
            'gromozeka.scheduler': {},
            'gromozeka.broker': {},
            'gromozeka.broker.consumer': {},
            'gromozeka.broker.consumer.*': {},
        }
    }

    config.dictConfig(LOGGING)