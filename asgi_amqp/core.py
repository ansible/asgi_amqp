from __future__ import unicode_literals

import pika
import six
import uuid
import msgpack
import logging
import random

from asgiref.base_layer import BaseChannelLayer

logging.getLogger('pika').propagate = False


class AMQPChannelLayer(BaseChannelLayer):
    def __init__(self, url=None, prefix='asgi:', expiry=60, group_expiry=86400, capacity=100, channel_capacity=None):
        super(AMQPChannelLayer, self).__init__(expiry, capacity, group_expiry, channel_capacity)
        self.url = url or 'amqp://guest:guest@localhost:5672/%2F'
        self.prefix = prefix
        self.exchange = prefix + 'channels'

    def send(self, channel, message):
        assert isinstance(message, dict), "message is not a dict"
        assert self.valid_channel_name(channel), "Channel name not valid"

        connection = self.connection()
        sendchan = connection.channel()

        sendchan.queue_declare(queue=channel, durable=False, exclusive=False)
        sendchan.basic_publish(exchange='',
                               routing_key=channel,
                               body=self.serialize(message),
                               properties=pika.BasicProperties(delivery_mode=1,content_type='text/plain'))
        connection.close()

    def receive_many(self, channels, block=False):
        if not channels:
            return None, None

        connection = self.connection()

        channels = list(channels)
        random.shuffle(channels)

        for channel in channels:
            try:
                recvchan = connection.channel()
                method_frame, header_frame, body = recvchan.basic_get(queue=channel)
                if method_frame:
                        recvchan.basic_ack(method_frame.delivery_tag)
                        if '!' in channel:
                            recvchan.queue_delete(channel)
                        connection.close()
                        return channel, self.deserialize(body)
            except pika.exceptions.ChannelClosed:
                continue

        if block:
            connection.sleep(0.1)
        connection.close()
        return None, None

    def new_channel(self, pattern):
        assert isinstance(pattern, six.text_type)
        assert pattern.endswith("!") or pattern.endswith("?")
        new_name = pattern + str(uuid.uuid4())
        return new_name

    def connection(self):
        return pika.BlockingConnection(pika.connection.URLParameters(self.url))

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
