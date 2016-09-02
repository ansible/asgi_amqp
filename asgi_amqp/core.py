from __future__ import unicode_literals

import pika
import six
import uuid

from asgiref.base_layer import BaseChannelLayer


class AMQPChannelLayer(BaseChannelLayer):
    #extensions = ["groups"]

    def __init__(self, url=None, expiry=60, group_expiry=86400, capacity=100, channel_capacity=None):
        super(AMQPChannelLayer).__init__(expiry, capacity, group_expiry, channel_capacity)
        self.url = url or 'amqp://guest:guest@localhost:5672/%2F'

    def send(self, channel, message):
        assert isinstance(message, dict), "message is not a dict"
        assert self.valid_channel_name(channel), "Channel name not valid"

        connection = self.connection()
        sendchan = connection.channel()
        sendchan.queue_declare(queue=channel)
        sendchan.basic_publish(exchange='', routing_key=channel, body=message)
        connection.close()

    def receive_many(self, channels, block=False):
        connection = self.connection()
        recvchan = connection.channel()
        while True:
            next_channel = self.next_channel(channels)
            method_frame, header_frame, body = recvchan.basic_get(next_channel)
            if method_frame:
                recvchan.basic_ack(method_frame.delivery_tag)
                return next_channel, self.deserialize(body)
            else:
                return None, None

    def next_channel(self, channels):
        for channel in channels:
            yield channel

    def new_channel(self, pattern):
        assert isinstance(pattern, six.text_type)
        assert pattern.endswith("!") or pattern.endswith("?")
        new_name = pattern + str(uuid.uuid4())
        return new_name

    def connection(self):
        return pika.BlockingConnection(pika.connection.URLParameters(self.url))

    def __str__(self):
        return "%s(host=%s)" % (self.__class__.__name__, self.host)
