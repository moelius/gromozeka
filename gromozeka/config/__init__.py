import logging
import os
from logging import config


def get_logging():
    from gromozeka.app import get_app
    app_id = get_app().config.app_id
    return {
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
            '%s' % app_id: {'handlers': ['console'], 'level': 'INFO'},
            '%s.pool' % app_id: {},
            '%s.pool.worker' % app_id: {},
            '%s.pool.worker.*' % app_id: {},
            '%s.scheduler' % app_id: {},
            '%s.broker' % app_id: {},
            '%s.broker.consumer' % app_id: {},
            '%s.broker.consumer.*' % app_id: {},
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
        backend_results_timelife(:obj:`int`): Results expiration time life (seconds). Default: 3600
    """

    def __init__(self):
        self.debug = False
        self.app_id = 'gromozeka'
        self.broker_url = 'amqp://guest:guest@localhost:5672/%2F'
        self.broker_reconnect_max_retries = 10
        self.broker_reconnect_retry_countdown = 10
        self.backend_url = 'redis://localhost'
        self.backend_reconnect_max_retries = 10
        self.backend_reconnect_retry_countdown = 10
        self.backend_results_timelife = 3600

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
        self._set_up_logging()
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
        self._set_up_logging()
        return self

    def _set_up_logging(self):
        config.dictConfig(get_logging())
        if self.debug:
            logging.getLogger(self.app_id).setLevel(logging.DEBUG)

    def _set_var_type(self, type_, name, attr):
        if type(None) == type_:
            setattr(self, name, attr)
        if type(False) == type_:
            setattr(self, name, False if attr in ('0', 0, 'false', 'False', '') else True)
        else:
            setattr(self, name, type_(attr))


__all__ = ['Config']
