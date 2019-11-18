# Copyright (c) 2017 Ansible by Red Hat
#  All Rights Reserved.

from __future__ import unicode_literals

import kombu
import six
import uuid
import msgpack
import socket
import threading
import datetime
import jsonpickle
import re
import urllib.parse

from asgiref.base_layer import BaseChannelLayer
from collections import deque
from kombu.pools import producers

from django.db import transaction
from django.conf import settings
from django.utils.module_loading import import_string


def normalize_broker_url(value):
    # see: https://github.com/ansible/awx/commit/6ff1fe8548745fd8e46a72c92055ef30f4c50cb9
    parts = value.rsplit('@', 1)
    match = re.search('(amqp://[^:]+:)(.*)', parts[0])
    if match:
        prefix, password = match.group(1), match.group(2)
        parts[0] = prefix + urllib.parse.quote(password)
    return '@'.join(parts)


class AMQPChannelLayer(BaseChannelLayer):
    def __init__(self, url=None, prefix='asgi:', expiry=60, group_expiry=86400, capacity=100, channel_capacity=None):

        try:
            init_func = import_string(self.config["INIT_FUNC"])
            init_func()
        except KeyError:
            pass
        except ImportError:
            raise RuntimeError("Cannot import INIT_FUNC")

        try:
            self.model = import_string(self.config["MODEL"])
        except KeyError:
            from .models import ChannelGroup
            self.model = ChannelGroup
        except ImportError:
            raise RuntimeError("Cannot import MODEL")

        super(AMQPChannelLayer, self).__init__(
            expiry=expiry,
            group_expiry=group_expiry,
            capacity=capacity,
            channel_capacity=channel_capacity,
        )

        if url:
            url = normalize_broker_url(url)
        self.url = url or 'amqp://guest:guest@localhost:5672/%2F'
        self.prefix = prefix + 'tower:websocket'
        self.exchange = kombu.Exchange(self.prefix, type='topic')

        self.tdata = threading.local()

    @property
    def config(self):
        return getattr(settings, "ASGI_AMQP", {})

    def _init_thread(self):
        if not hasattr(self.tdata, 'connection'):
            self.tdata.connection = kombu.Connection(self.url)
            self.tdata.connection.default_channel.basic_qos(0, 1, False)
        if not hasattr(self.tdata, 'consumer'):
            self.tdata.consumer = self.tdata.connection.Consumer([], callbacks=[self.on_message],
                                                                 accept=['msgpack', 'application/msgpack'],
                                                                 no_ack=True)
        if not hasattr(self.tdata, 'buffer'):
            self.tdata.buffer = deque()

        if not hasattr(self.tdata, 'routing_keys'):
            self.tdata.routing_keys = set()

    def recover(self):
        self._init_thread()
        self.tdata.connection.ensure_connection(max_retries=5)
        self.tdata.consumer.revive(self.tdata.connection.default_channel)

    def send(self, channel, message):
        self._init_thread()

        assert isinstance(message, dict), "message is not a dict"
        assert self.valid_channel_name(channel)

        def do_send():
            with producers[self.tdata.connection].acquire(block=True) as producer:
                routing_key = channel_to_routing_key(channel)
                payload = self.serialize(message)
                producer.publish(payload, exchange=self.exchange, routing_key=routing_key, delivery_mode=1,
                                 content_type='application/msgpack', content_encoding='binary')

        try:
            do_send()
        except self.tdata.connection.recoverable_connection_errors:
            self.recover()
            do_send()

    def receive(self, channels, block=False):
        if not channels:
            return None, None

        self._init_thread()

        # bind this queue to messages sent to any of the routing_keys
        # in the channels set.
        incoming_routing_keys = [channel_to_routing_key(channel) for channel in channels]
        new_routing_keys = set(incoming_routing_keys).difference(self.tdata.routing_keys)
        self.tdata.routing_keys = new_routing_keys.union(self.tdata.routing_keys)

        for nrk in new_routing_keys:
            if nrk.endswith('.'):
                nrk += '*'
            queue = kombu.Queue(name=self.prefix+':{}'.format(nrk),
                                exchange=self.exchange, durable=False, exclusive=False,
                                auto_delete=True, routing_key=nrk)
            self.tdata.consumer.add_queue(queue)
        self.tdata.consumer.consume()

        while True:
            # check local buffer for messages
            if self.tdata.buffer:
                message = self.tdata.buffer.popleft()
                channel = routing_key_to_channel(message.delivery_info['routing_key'])
                return channel, self.deserialize(message.body)

            try:
                self.tdata.connection.drain_events(timeout=1)
            except socket.timeout:
                break
            except self.tdata.connection.recoverable_connection_errors:
                self.recover()
                self.tdata.consumer.consume()

        return None, None

    def on_message(self, body, message):
        self.tdata.buffer.append(message)

    def new_channel(self, pattern):
        assert isinstance(pattern, six.text_type)
        assert pattern.endswith("!") or pattern.endswith("?")
        new_name = pattern + str(uuid.uuid4())
        return new_name

    def group_add(self, group, channel):
        with transaction.atomic():
            g, created = self.model.objects.select_for_update().get_or_create(group=group)
            ts = datetime.datetime.utcnow()

            if created:
                channels = {channel: ts}
            else:
                channels = jsonpickle.decode(g.channels)
                channels.update({channel: ts})

            for c, ts in list(channels.items()):
                now = datetime.datetime.utcnow()
                if (now - ts).total_seconds() > self.group_expiry:
                    del channels[c]

            g.channels = jsonpickle.encode(channels)
            g.save()

    def group_discard(self, group, channel):
        with transaction.atomic():
            try:
                g = self.model.objects.select_for_update().get(group=group)
            except self.model.DoesNotExist:
                return None

            channels = jsonpickle.decode(g.channels)
            if channel in channels:
                del channels[channel]

            g.channels = jsonpickle.encode(channels)
            g.save()

    def group_channels(self, group):
        try:
            g = self.model.objects.get(group=group)
        except self.model.DoesNotExist:
            return {}

        channels = jsonpickle.decode(g.channels)
        return channels.keys()

    def send_group(self, group, message):
        for channel in self.group_channels(group):
            self.send(channel, message)

    def serialize(self, message):
        """Serializes message to a byte string."""
        value = msgpack.packb(message, use_bin_type=True)
        return value

    def deserialize(self, message):
        """Deserializes from a byte string."""
        return msgpack.unpackb(message, encoding="utf8")

    def __str__(self):
        return "%s(host=%s)" % (self.__class__.__name__, self.host)


def routing_key_to_channel(routing_key):
    if routing_key.count('.') == 3:
        routing_key = routing_key[::-1].replace('.', '!', 1)[::-1]
    return routing_key


def channel_to_routing_key(channel):
    return channel.replace('!', '.', 1)
