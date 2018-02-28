asgi_amqp
==========

An ASGI channel layer that uses AMQP as its backing store with group support.

Settings
--------

The `asgi_amqp` channel layer looks for settings in `ASGI_AMQP` and
has the following configuration options. URL and connection settings
are configured through `CHANNEL_LAYER` same as any channel layer.

**MODEL**
Set a custom `ChannelGroup` model to use. See more about this in the ChannelGroup
Model section of this README.

Usage::

    ASGI_AMQP = {'MODEL': 'awx.main.models.channels.ChannelGroup'}

**INIT_FUNC**
A function that you want run when the channel layer is first instantiated.

Usage::

    ASGI_AMQP = {'INIT_FUNC': 'awx.prepare_env'}


ChannelGroup Model
------------------

This channel layer requires a database model called `ChannelGroup`. You
can use the model and migation provided by adding `asgi_amqp` to your
installed apps or you can point the `ASGI_AMQP.MODEL` setting to a
model you have already defined.

Installed Apps::

    INSTALLED_APPS = [
        ...
        'asgi_amqp',
        ...
    ]

Settings::

    ASGI_AMQP = {
        'MODEL': 'awx.main.models.channels.ChannelGroup',
    }
