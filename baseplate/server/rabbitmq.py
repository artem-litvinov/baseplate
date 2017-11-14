from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

import signal
import gevent
from kombu.mixins import ConsumerMixin
from baseplate.integration.rabbitmq import (connection_from_config,
                                            queue_from_config)

from ..config import Integer, parse_config


class RabbitServer(ConsumerMixin):
    """
    An extension of kombu's ConsumerMixin class compatible
    with gevent's BaseServer interface
    """

    def __init__(self, conn, queues, consumer_factory, **kwargs):
        self.connection = conn
        self.queues = queues
        self.consumer_factory = consumer_factory

        if "max_retries" in kwargs:
            self.connect_max_retries = kwargs["max_retries"]

    def get_consumers(self, _, channel):
        return self.consumer_factory.get_consumers(channel, self.queues)

    def serve_forever(self):
        """
        Start consuming data from RabbitMQ queue using
        provided consumer and wait until it's stopped.
        """

        signal.signal(signal.SIGINT, lambda sig, frame: self.stop())
        signal.signal(signal.SIGTERM, lambda sig, frame: self.stop())

        with self.connection:
            # 'run' method will stop iterating over incoming messages when
            # stop() is called and return after the last message processed
            worker = gevent.spawn(self.run)
            worker.join()

    def stop(self):
        # This will instruct consumer to stop processing incoming messages
        self.should_stop = True


def make_server(config, _, app):
    queue = queue_from_config(config)
    connection = connection_from_config(config)

    max_retries = parse_config(config, {
        "rabbit": {
            "connection_max_retries": Integer,
        },
    }).rabbit.connection_max_retries

    server = RabbitServer(
        conn=connection,
        queues=[queue],
        consumer_factory=app,
        max_retries=max_retries)

    return server
