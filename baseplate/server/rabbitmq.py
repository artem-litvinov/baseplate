from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

import signal
import gevent

from baseplate.integration.rabbitmq import connection_from_config, queue_from_config
from kombu.mixins import ConsumerMixin

class RabbitServer(ConsumerMixin):

    def __init__(self, conn, queues, handler, listener, **kwargs):
        self.connection = conn
        self.queues = queues
        self.__callbacks = handler.get_callbacks()

        if "max_retries" in kwargs:
            self.connect_max_retries = kwargs["max_retries"]

    def get_consumers(self, Consumer, channel):
        return [
            Consumer(queues=self.queues, callbacks=self.__callbacks),
        ]

    def serve_forever(self, stop_timeout=None):
        """Start consuming data from RabbitMQ queue using provided consumer and wait until it's stopped."""

        print("SERVE_FOREVER!!!!!")

        signal.signal(signal.SIGINT, lambda sig, frame: self.stop())
        signal.signal(signal.SIGTERM, lambda sig, frame: self.stop())

        with self.connection as connection:
            # 'run' method will stop iterating over incoming messages when
            # stop() is called and return after the last message processed
            worker = gevent.spawn(self.run)
            worker.join()

    def stop(self):
        # This will instruct consumer to stop processing incoming messages
        self.should_stop = True
        print("STOP")


def make_server(config, listener, app):

    queue = queue_from_config(config)
    connection = connection_from_config(config)

    from ..config import parse_config, Integer

    max_retries = parse_config(config, {
        "rabbit": {
            "connection_max_retries": Integer,
        },
    }).rabbit.connection_max_retries

    server = RabbitServer(
        conn=connection,
        queues=[queue],
        handler=app,
        listener=listener, max_retries = max_retries)

    return server
