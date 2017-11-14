from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

from kombu import Connection, Exchange, Queue
from kombu.pools import Producers

from thrift.util.Serializer import serialize

from . import ContextFactory
from .. import config


def connection_from_config(app_config, prefix="rabbitmq.", **kwargs):
    """Make a kombu connection from a configuration dictionary.

    ---

    """
    assert prefix.endswith(".")
    config_prefix = prefix[:-1]
    cfg = config.parse_config(app_config, {
        config_prefix: {
            "url": config.String,
        },
    })

    options = getattr(cfg, config_prefix)

    return Connection(options.url)

def make_exchange(*args, **kwargs):
    return Exchange(*args, **kwargs)


def make_queue(*args, **kwargs):
    return Queue(*args, **kwargs)

class RabbitMQPublisherContextFactory(ContextFactory):
    """RabbitMQ publisher context factory.

    ---

    """
    def __init__(self, connection, max_connections=None):
        self.connection = connection
        self.producers = Producers(limit=max_connections)

    def make_object_for_context(self, name, span):
        return RabbitMQPublisher(name, span, self.connection, self.producers)


class RabbitMQPublisher(object):
    def __init__(self, name, span, connection, producers):
        self.name = name
        self.span = span
        self.connection = connection
        self.producers = producers

    def publish(self, *args, **kwargs):
        trace_name = "{}.{}".format(self.name, "publish")
        child_span = self.span.make_child(trace_name)

        child_span.set_tag("kind", "producer")
        routing_key = kwargs.get("routing_key")
        if routing_key:
            child_span.set_tag("message_bus.destination", routing_key)

        with child_span:
            producer_pool = self.producers[self.connection]
            with producer_pool.acquire(block=True) as producer:
                return producer.publish(*args, **kwargs)

    def publish_with_thrift_serialization(self, body, *args, **kwargs):
        serialized_body = serialize(body)
        return self.publish(serialized_body, *args, **kwargs)

    def get_channel(self):
        return self.connection.channel()
