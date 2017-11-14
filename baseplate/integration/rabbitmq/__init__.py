from ... import config

import kombu
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
    return kombu.Queue(**options)


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
    return kombu.Exchange(**options)

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
