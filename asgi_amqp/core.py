from __future__ import unicode_literals

import kombu
import six
import uuid
import msgpack
import socket
import threading
import datetime
import jsonpickle

from asgiref.base_layer import BaseChannelLayer
from collections import deque
from kombu.pools import producers

import awx
awx.prepare_env()

from awx.main.models import ChannelGroup


class AMQPChannelLayer(BaseChannelLayer):
    def __init__(self, url=None, prefix='asgi:', expiry=60, group_expiry=86400, capacity=100, channel_capacity=None):
        super(AMQPChannelLayer, self).__init__(
            expiry=expiry,
            group_expiry=group_expiry,
            capacity=capacity,
            channel_capacity=channel_capacity,
        )

        kombu.serialization.enable_insecure_serializers()

        self.url = url or 'amqp://guest:guest@localhost:5672/%2F'
        self.prefix = prefix + 'tower:websocket'
        self.exchange = kombu.Exchange(self.prefix, type='topic')

        self.tdata = threading.local()

    def _init_thread(self):
        if not hasattr(self.tdata, 'connection'):
            self.tdata.connection = kombu.Connection(self.url)
            self.tdata.connection.default_channel.basic_qos(0, 1, False)
            self.tdata.consumer = self.tdata.connection.Consumer([], callbacks=[self.on_message], no_ack=False)

        if not hasattr(self.tdata, 'buffer'):
            self.tdata.buffer = deque()

        if not hasattr(self.tdata, 'routing_keys'):
            self.tdata.routing_keys = set()

    def send(self, channel, message):
        self._init_thread()

        assert isinstance(message, dict), "message is not a dict"
        assert self.valid_channel_name(channel)

        with producers[self.tdata.connection].acquire(block=True) as producer:
            routing_key = channel_to_routing_key(channel)
            payload = self.serialize(message)
            producer.publish(payload, exchange=self.exchange, routing_key=routing_key, delivery_mode=1,
                             content_type='application/msgkpack', content_encoding='binary')

    def receive_many(self, channels, block=False):
        if not channels:
            return None, None

        self._init_thread()

        # bind this queue to messages sent to any of the routing_keys
        # in the channels set.
        incoming_routing_keys = routing_keys_from_channels(channels)
        new_routing_keys = set(incoming_routing_keys).difference(self.tdata.routing_keys)
        self.tdata.routing_keys = new_routing_keys.union(self.tdata.routing_keys)

        for nrk in new_routing_keys:
            queue = kombu.Queue(name=self.prefix+':{}'.format(nrk),
                                exchange=self.exchange, durable=True, exclusive=False,
                                routing_key=nrk)
            self.tdata.consumer.add_queue(queue)
        self.tdata.consumer.consume()

        while True:
            # check local buffer for messages
            if self.tdata.buffer:
                message = self.tdata.buffer.popleft()
                channel = routing_key_to_channel(message.delivery_info['routing_key'])
                message.ack()
                return channel, self.deserialize(message.body)
            try:
                self.tdata.connection.drain_events(timeout=1)
            except socket.timeout:
                break

        return None, None

    def on_message(self, body, message):
        self.tdata.buffer.append(message)

    def new_channel(self, pattern):
        assert isinstance(pattern, six.text_type)
        assert pattern.endswith("!") or pattern.endswith("?")
        new_name = pattern + str(uuid.uuid4())
        return new_name

    def group_add(self, group, channel):
        g, created = ChannelGroup.objects.get_or_create(group=group)
        ts = datetime.datetime.utcnow()

        if created:
            channels = {channel: ts}
        else:
            channels = jsonpickle.decode(g.channels)
            channels.update({channel: ts})

        for c, ts in channels.items():
            now = datetime.datetime.utcnow()
            if (now - ts).total_seconds() > self.group_expiry:
                del channels[c]

        g.channels = jsonpickle.encode(channels)
        g.save()

    def group_discard(self, group, channel):
        try:
            g = ChannelGroup.objects.get(group=group)
        except ChannelGroup.DoesNotExist:
            return None

        channels = jsonpickle.decode(g.channels)
        if channel in channels:
            del channels[channel]

        g.channels = jsonpickle.encode(channels)
        g.save()

    def group_channels(self, group):
        try:
            g = ChannelGroup.objects.get(group=group)
        except ChannelGroup.DoesNotExist:
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
    if routing_key.count('.') == 2:
        routing_key = routing_key[::-1].replace('.', '!', 1)[::-1]
    return routing_key


def channel_to_routing_key(channel):
    return channel.replace('!', '.', 1)


def routing_keys_from_channels(channels):
    routing_keys = set()
    for channel in channels:
        routing_key = channel.split('!')[0]
        if '!' in channel:
            routing_key += ".*"
        routing_keys.add(routing_key)
    return routing_keys
