from confluent_kafka import Consumer

from .config import Config


def build_consumer(cfg: Config) -> Consumer:
    conf = {
        "bootstrap.servers": cfg.brokers,
        "group.id": cfg.group_id,
        "enable.auto.commit": False,
        "auto.offset.reset": "latest",
        "isolation.level": "read_committed",
    }
    # TODO Add SASL_SSL if cfg.sasl_username statement
    return Consumer(conf)
