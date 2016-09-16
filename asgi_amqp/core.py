from __future__ import unicode_literals

import kombu
import six
import uuid
import msgpack
import socket

from asgiref.base_layer import BaseChannelLayer
from collections import deque


class AMQPChannelLayer(BaseChannelLayer):
    def __init__(self, url=None, prefix='asgi:', expiry=60, group_expiry=86400, capacity=100, channel_capacity=None):
        super(AMQPChannelLayer, self).__init__(expiry, capacity, group_expiry, channel_capacity)
        self.url = url or 'amqp://guest:guest@localhost:5672/%2F'
        self.prefix = prefix
        self.exchange_name = self.prefix + 'tower'
        self.exchange = kombu.Exchange(self.exchange_name, type='direct')
        self._buffer = deque()

        kombu.serialization.enable_insecure_serializers()

    def send(self, channel, message):
        assert isinstance(message, dict), "message is not a dict"
        assert self.valid_channel_name(channel), "Channel name not valid"
        # send a message to the asgi:tower exchange
        # use the channel as the routing_key
        with kombu.Connection(self.url) as connection:
            payload = self.serialize(message)
            producer = kombu.Producer(connection, self.exchange, routing_key=channel)
            producer.publish(payload, routing_key=channel, delivery_mode=2,
                                content_type='application/msgkpack', content_encoding='binary')

    def receive_many(self, channels, block=False):
        if not channels:
            return None, None

        # bind this queue to messages sent to any of the routing_keys
        # in the channels set.
        queue = kombu.Queue(name='asgi:tower', exchange=self.exchange, durable=True, exclusive=False,
                            bindings=[kombu.binding(self.exchange, routing_key=channel)
                                      for channel in channels])

        with kombu.Connection(self.url) as connection:
            # set the qos prefetch to 1 to ensure that we handle one message
            # from the consumer as a time.
            connection.default_channel.basic_qos(0, 1, False)
            with kombu.Consumer(connection, queues=[queue], callbacks=[self.on_message]) as consumer:
                while True:
                    # check our local buffer for a message
                    if self._buffer:
                        # if we've polled a message, ack the message
                        channel, message = self._buffer.popleft()
                        message.ack()

                        payload = self.deserialize(message.body)
                        return channel, payload
                    try:
                        # poll for a message from the consumer
                        consumer.qos(prefetch_count=1)
                        connection.drain_events(timeout=1)
                    except socket.timeout:
                        # no messages found
                        return None, None

    def on_message(self, body, message):
        channel = message.delivery_info['routing_key']
        self._buffer.append((channel, message))

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
