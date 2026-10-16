"""
Unit tests for src/kafka.py.

`confluent_kafka.Consumer` is replaced so no broker connection is attempted.
"""

from unittest.mock import create_autospec, patch

import pytest
from confluent_kafka import Consumer
from src import kafka as kafka_mod
from src.config import Config
from src.kafka import build_consumer


@pytest.fixture
def cfg() -> Config:
    return Config(
        brokers="broker-1:9092,broker-2:9092",
        group_id="protobs-tap",
        topics=["orders"],
    )


@pytest.fixture
def consumer_cls():
    with patch.object(kafka_mod, "Consumer", create_autospec(Consumer)) as consumer_cls:
        yield consumer_cls


class TestBuildConsumer:
    def test_configures_manual_commit_read_committed_consumer(self, cfg, consumer_cls):
        result = build_consumer(cfg)

        consumer_cls.assert_called_once_with(
            {
                "bootstrap.servers": "broker-1:9092,broker-2:9092",
                "group.id": "protobs-tap",
                # The tap commits itself, only after the sink write succeeds.
                "enable.auto.commit": False,
                "auto.offset.reset": "latest",
                "isolation.level": "read_committed",
            }
        )
        assert result is consumer_cls.return_value

    def test_never_enables_auto_commit(self, cfg, consumer_cls):
        # Guards the at-least-once guarantee: auto-commit could acknowledge
        # offsets before the sink write succeeds.
        build_consumer(cfg)

        (conf,) = consumer_cls.call_args.args
        assert conf["enable.auto.commit"] is False

    def test_does_not_subscribe_or_poll(self, cfg, consumer_cls):
        build_consumer(cfg)

        consumer = consumer_cls.return_value
        consumer.subscribe.assert_not_called()
        consumer.poll.assert_not_called()
