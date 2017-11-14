from ... import config

from thrift.TSerialization import serialize

import kombu
from kombu.pools import Producers
from kombu.mixins import ConsumerMixin


class BaseplateConsumerBase(ConsumerMixin):
    pass


class BaseplateConsumerBuilder(object):
    pass


def queue_from_config(app_config, prefix="rabbit.", **kwargs):
    print (app_config)
    assert prefix.endswith(".")
    config_prefix = prefix[:-1]
    # TODO: other config parameters
    cfg = config.parse_config(app_config, {
        config_prefix: {
            "queue_name": config.String,
            "queue_durable": config.Optional(config.Boolean, False),
            "queue_exclusive": config.Optional(config.Boolean, False),
        },
    })

    options = getattr(cfg, config_prefix)
    # get all prefs, prefixed with queue_ and build Queue kwargs
    options = dict((k.replace("queue_", ""),v) for k,v in options.items() if k.startswith("queue"))
    return make_queue(**options)


def make_queue(*args, **kwargs):
    return kombu.Queue(*args, **kwargs)


def exchange_from_config(app_config, prefix="rabbit.", **kwargs):
    assert prefix.endswith(".")
    config_prefix = prefix[:-1]
    cfg = config.parse_config(app_config, {
        config_prefix: {
            "exchange_name": config.String,
            # TODO: enum?
            "exchange_type": config.String,
        },
    })

    options = getattr(cfg, config_prefix)
    return make_exchange(**options)


def make_exchange(*args, **kwargs):
    return kombu.Exchange(*args, **kwargs)


def connection_from_config(app_config, prefix="rabbit.", **kwargs):
    assert prefix.endswith(".")
    config_prefix = prefix[:-1]
    cfg = config.parse_config(app_config, {
        config_prefix: {
            # TODO: config.Endpoint?
            "connection_url": config.String,
        },
    })

    url = getattr(cfg, config_prefix).connection_url

    return kombu.Connection(url)


class RabbitMQPublisherContextFactory(ContextFactory):
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
