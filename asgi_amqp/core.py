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
from collections import (
    deque,
    defaultdict,
)
from kombu.pools import producers


class Group(object):
    def __init__(self, expiry=86400):
        self.expiry = expiry
        self.members = {}

    def add(self, member, ts=None):
        self.members[member] = ts or datetime.datetime.utcnow()

    def remove(self, member):
        if member in self.members:
            del self.members[member]

    def channels(self):
        self.expire()
        return self.members.keys()

    def expire(self, expiry=None):
        exp = expiry or self.expiry
        for k, v in self.members.items():
            now = datetime.datetime.now()
            if (now - v).total_seconds() > exp:
                del self.members[k]


def group_factory():
    return Group()


def update_groups(func):
    def wrapper(channel_layer, *args, **kwargs):
        channel = channel_layer.tdata.connection.channel()
        group_queue_id = 'group-' + str(uuid.uuid4())
        queue = kombu.Queue(name=channel_layer.prefix + ':{}'.format(group_queue_id), exchange=channel_layer.group_exchange,
                            channel=channel, no_ack=True, durable=False, exclusive=True)
        queue.queue_declare()
        message = queue.get(no_ack=True)
        if message:
            message = channel_layer.deserialize(message)
            groups = jsonpickle.decode(message)
            channel_layer.groups.update(groups)
        queue.delete(if_empty=True)
        channel.close()

        with producers[channel_layer.tdata.connection].acquire(block=True) as producer:
            groups = jsonpickle.encode(channel_layer.groups)
            payload = channel_layer.serialize(groups)
            producer.maybe_declare(channel_layer.group_exchange)
            producer.publish(payload, exchange=channel_layer.group_exchange, delivery_mode=1,
                            content_type='application/msgpack', content_encoding='binary')

        return func(channel_layer, *args, **kwargs)
    return wrapper


class AMQPChannelLayer(BaseChannelLayer):
    def __init__(self, url=None, prefix='asgi:', expiry=60, group_expiry=86400, capacity=100, channel_capacity=None):
        super(AMQPChannelLayer, self).__init__(expiry, capacity, group_expiry, channel_capacity)
        kombu.serialization.enable_insecure_serializers()

        self.url = url or 'amqp://guest:guest@localhost:5672/%2F'
        self.prefix = prefix + 'tower:{}'.format(socket.gethostname())
        self.exchange = kombu.Exchange(self.prefix, type='topic')

        self.groups = defaultdict(group_factory)
        self.group_exchange = kombu.Exchange('asgi:tower:groups', type='x-lvc')

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
        assert self.valid_channel_name(channel), "Channel name not valid"
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

    @update_groups
    def group_add(self, group, channel):
        g = self.groups[group]
        g.add(channel)

    @update_groups
    def group_discard(self, group, channel):
        g = self.groups[group]
        g.remove(channel)

    def group_channels(self, group):
        g = self.groups[group]
        g.expire()
        return g.channels()

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
