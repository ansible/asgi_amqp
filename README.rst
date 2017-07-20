asgi_amqp
==========

You should probably use [asgi_rabbitmq](http://asgi-rabbitmq.readthedocs.io/en/latest/index.html)
----------

Not intended for public consumption. Currently depends on closed source libraries.

An ASGI channel layer that uses AMQP as its backing store with group support.

*IMPORTANT*
-----------

This library expects your Django project to have a model called ChannelGroup.
You will need to fix the import of `ChannelGroup` in the code to make it work
with your django project.

Eventually I make this part of the configuration options so you can just pass
in `project.model.MyModel` in your `settings.py` file.


Example Model::

    from django.db import models

    class ChannelGroup(models.Model):
        group = models.CharField(max_length=200, unique=True)
        channels = models.TextField()


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
