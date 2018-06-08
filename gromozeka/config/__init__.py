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
        'gromozeka.pool.worker.*': {},
        'gromozeka.scheduler': {},
        'gromozeka.broker': {},
        'gromozeka.broker.consumer': {},
        'gromozeka.broker.consumer.*': {},
    }
}


class Config:
    """Configuration class
    
    Attributes:
        debug(:obj:`bool`): Set logger level. Default: `False`
        broker_reconnect_max_retries(:obj:`int`): Broker reconnect attempts. Default: 10
        broker_reconnect_retry_countdown(:obj:`int`): Sleep time between reconnects (seconds). Default: 10
        broker_url(:obj:`str`): Broker url. Default: 'amqp://guest:guest@localhost:5672/%2F'
        backend_url(:obj:`str`): Backend url. Default: 'redis://localhost'
        backend_reconnect_max_retries(:obj:`int`): Backend reconnect attempts. Default: 10
        backend_reconnect_retry_countdown(:obj:`int`): Sleep time between reconnects (seconds). Default: 10
    """

    def __init__(self):
        self.debug = False
        self.broker_url = 'amqp://guest:guest@localhost:5672/%2F'
        self.broker_reconnect_max_retries = 10
        self.broker_reconnect_retry_countdown = 10
        self.backend_url = 'redis://localhost'
        self.backend_reconnect_max_retries = 10
        self.backend_reconnect_retry_countdown = 10
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
            setattr(self, name, False if attr in ('0', 0, 'false', 'False', '') else True)
        else:
            setattr(self, name, type_(attr))


__all__ = ['Config']
