# Copyright (c) 2017 Ansible by Red Hat
# All Rights Reserved.

import pkg_resources
from .core import AMQPChannelLayer # noqa

__version__ = pkg_resources.require('asgi_amqp')[0].version
