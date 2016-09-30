asgi_amqp
==========

An ASGI channel layer that uses AMQP as its backing store with group support.

Installation
------------

Group support requires the `rabbitmq_lvc` plugin is enabled on your RabbitMQ server.
More information about enabling this plugin can be found on the (RabbitMQ Plugin)[https://www.rabbitmq.com/community-plugins.html] site.

     $ rabbitmq-plugins enable rabbitmq_lvc
     $ pip install asgi_amqp

Usage
-----

You'll need to instantiate the channel layer with at least ``url``,
and other options if you need them.

Example::

    channel_layer = AMQPChannelLayer(
        url="amqp://guest:guest@localhost:5672//",
        }
    )

host
~~~~

The server to connect to as a URL.
