import logging
import os
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
        'gromozeka.scheduler': {},
        'gromozeka.broker': {},
        'gromozeka.broker.consumer': {},
    }
}


class Config:
    """Configuration class
    
    Attributes:
        debug(:obj:`bool`): Set logger level. Default: `False`
        broker_reconnect_max_retries(:obj:`int`): Broker reconnect attempts. Default: 10
        broker_reconnect_retry_countdown(:obj:`int`): Sleep time between reconnects (seconds). Default: 10
        broker_retry_exchange(:obj:`str`): Broker exchange for retries. Default: 'countdown_exchange'
        broker_retry_queue(:obj:`str`): Broker queue name for retries. Default: 'countdown_queue'
        broker_retry_routing_key(:obj:`str`): Broker routing key for retries. Default: 'countdown'
        broker_eta_exchange(:obj:`str`): Broker exchange for eta tasks. Default: 'eta_exchange'
        broker_eta_queue(:obj:`str`): Broker queue for retries. Default: 'eta_queue'
        broker_eta_routing_key(:obj:`str`): Broker routing key for retries. Default: 'eta'
        broker_url(:obj:`str`): Broker url. Default: 'amqp://guest:guest@localhost:5672/%2F'
    """

    def __init__(self):
        self.debug = False
        self.broker_reconnect_max_retries = 10
        self.broker_reconnect_retry_countdown = 10
        self.broker_retry_exchange = 'countdown_exchange'
        self.broker_retry_queue = 'countdown_queue'
        self.broker_retry_routing_key = 'countdown'
        self.broker_eta_exchange = 'eta_exchange'
        self.broker_eta_queue = 'eta_queue'
        self.broker_eta_routing_key = 'eta'
        self.broker_url = 'amqp://guest:guest@localhost:5672/%2F'
        config.dictConfig(LOGGING)

    def from_env(self):
        """Configure Gromozeka with environment variables

        Returns:
            gromozeka.Config: Configured object
        """

        for name in dir(self):
            if name.startswith('__') or callable(getattr(self, name)):
                continue
            new_attr = os.getenv(name, getattr(self, name))
            type_ = type(getattr(self, name))
            self._set_var_type(type_, name, new_attr)
        if self.debug:
            logging.getLogger('gromozeka').setLevel(logging.DEBUG)

        return self

    def from_dict(self, config_dict):
        """Configure Gromozeka with :obj:`dict`

        Args:
            config_dict(dict): config :obj:`dict`

        Returns:
            gromozeka.Config: Configured object
        """
        for name, value in config_dict.items():
            if not hasattr(self, name):
                from gromozeka import GromozekaException
                raise GromozekaException('Unknown config option `{}`'.format(name))
            type_ = type(getattr(self, name))
            self._set_var_type(type_, name, value)
        if self.debug:
            logging.getLogger('gromozeka').setLevel(logging.DEBUG)

        return self

    def _set_var_type(self, type_, name, attr):
        if type(None) == type_:
            setattr(self, name, attr)
        if type(False) == type_:
            if attr in ('0', 0, 'false', 'False', ''):
                setattr(self, name, False)
            else:
                setattr(self, name, True)
        else:
            setattr(self, name, type_(attr))


__all__ = ['Config']
