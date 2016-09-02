import pkg_resources
from .core import AMQPChannelLayer

__version__ = pkg_resources.require('asgi_amqp')[0].version
