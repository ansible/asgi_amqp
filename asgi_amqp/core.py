from __future__ import unicode_literals

import kombu
import six
import uuid
import msgpack
import socket

from asgiref.base_layer import BaseChannelLayer
from collections import deque
from kombu.pools import producers


class AMQPChannelLayer(BaseChannelLayer):
    def __init__(self, url=None, prefix='asgi:', expiry=60, group_expiry=86400, capacity=100, channel_capacity=None):
        super(AMQPChannelLayer, self).__init__(expiry, capacity, group_expiry, channel_capacity)
        kombu.serialization.enable_insecure_serializers()

        self._buffer = deque()
        self.url = url or 'amqp://guest:guest@localhost:5672/%2F'
        self.prefix = prefix + 'tower:{}'.format(socket.gethostname())

        self.connection = kombu.Connection(self.url)
        self.connection.default_channel.basic_qos(0, 1, False)
        self.pool = self.connection.ChannelPool(1)

        self.channel = self.connection.default_channel
        self.exchange = kombu.Exchange(self.prefix, type='topic', channel=self.channel)

    def send(self, channel, message):
        assert isinstance(message, dict), "message is not a dict"
        assert self.valid_channel_name(channel), "Channel name not valid"
        with producers[self.connection].acquire(block=True) as producer:
            routing_key = channel_to_routing_key(channel)
            payload = self.serialize(message)
            producer.publish(payload, exchange=self.exchange, routing_key=routing_key, delivery_mode=1,
                             content_type='application/msgkpack', content_encoding='binary')

    def receive_many(self, channels, block=False):
        if not channels:
            return None, None

        # bind this queue to messages sent to any of the routing_keys
        # in the channels set.
        routing_keys = routing_keys_from_channels(channels)
        queues = [kombu.Queue(name=self.prefix+':{}'.format(rk),
                              exchange=self.exchange, durable=True, exclusive=False,
                              channel=self.channel,
                              routing_key=rk) for rk in routing_keys]
        consumer = kombu.Consumer(self.channel, queues, callbacks=[self.on_message], no_ack=False)
        consumer.consume()

        while True:
            # check local buffer for messages
            if self._buffer:
                message = self._buffer.popleft()
                channel = routing_key_to_channel(message.delivery_info['routing_key'])
                if channel in channels:
                    # ack the local message and return it to be handled by channels
                    message.ack()
                    return channel, self.deserialize(message.body)
                else:
                    message.requeue()
                    break
            try:
                self.connection.drain_events(timeout=1)
            except socket.timeout:
                break
        return None, None

    def on_message(self, body, message):
        self._buffer.append(message)

    def new_channel(self, pattern):
        assert isinstance(pattern, six.text_type)
        assert pattern.endswith("!") or pattern.endswith("?")
        new_name = pattern + str(uuid.uuid4())
        return new_name

    def serialize(self, message):
        """
        Serializes message to a byte string.
        """
        value = msgpack.packb(message, use_bin_type=True)
        return value

    def deserialize(self, message):
        """
        Deserializes from a byte string.
        """
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
