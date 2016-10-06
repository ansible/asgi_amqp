asgi_amqp
==========

Not intended for public consumption. Currently depends on closed source libraries.

An ASGI channel layer that uses AMQP as its backing store with group support.

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
