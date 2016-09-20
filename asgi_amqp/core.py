from __future__ import unicode_literals

import kombu
import six
import uuid
import msgpack
import socket

from asgiref.base_layer import BaseChannelLayer
from collections import deque
from kombu.pools import (
    connections,
    producers,
)

class AMQPChannelLayer(BaseChannelLayer):
    def __init__(self, url=None, prefix='asgi:', expiry=60, group_expiry=86400, capacity=100, channel_capacity=None):
        super(AMQPChannelLayer, self).__init__(expiry, capacity, group_expiry, channel_capacity)
        self.url = url or 'amqp://guest:guest@localhost:5672/%2F'
        self.prefix = prefix
        self.exchange_name = self.prefix + 'tower'
        self.exchange = kombu.Exchange(self.exchange_name, type='topic')

        self.connection = kombu.Connection(self.url)
        self.connection.default_channel.basic_qos(0, 1, False)

        self._buffer = deque()

        kombu.serialization.enable_insecure_serializers()

    def send(self, channel, message):
        assert isinstance(message, dict), "message is not a dict"
        assert self.valid_channel_name(channel), "Channel name not valid"
        with producers[self.connection].acquire(block=True) as producer:
            routing_key = channel.replace('!','.',1)
            payload = self.serialize(message)
            producer.publish(payload, exchange=self.exchange, routing_key=routing_key, delivery_mode=2,
                             content_type='application/msgkpack', content_encoding='binary')

            print("SEND", routing_key)

    def receive_many(self, channels, block=False):
        if not channels:
            return None, None

        # bind this queue to messages sent to any of the routing_keys
        # in the channels set.
        routing_keys = set()
        for channel in channels:
            routing_key = channel.split('!')[0]
            if '!' in channel:
                routing_key += ".*"
            routing_keys.add(routing_key)

        queues = [kombu.Queue(name=self.prefix+'tower'+':{}'.format(rk),
                              exchange=self.exchange, durable=True, exclusive=False,
                              routing_key=rk) for rk in routing_keys]

        with connections[self.connection].acquire(block=True) as connection:
            with kombu.Consumer(connection, queues=queues, callbacks=[self.on_message]) as consumer:
                while True:
                    # check our local buffer for a message
                    if self._buffer:
                        # if we've polled a message, ack the message
                        message = self._buffer.popleft()
                        channel = message.delivery_info['routing_key']
                        if channel.count('.') == 2:
                            channel = channel[::-1].replace('.','!',1)[::-1]

                        print("CHANNELS", channel, channels)
                        if channel in channels:
                            message.ack()
                            consumer.recover(requeue=True)
                            return channel, self.deserialize(message.body)
                        else:
                            message.requeue()
                            return None, None
                    try:
                        # poll for a message from the consumer
                        consumer.qos(prefetch_count=1)
                        connection.drain_events(timeout=1)
                    except socket.timeout:
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
