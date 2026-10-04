import os
import time
from enum import Enum

import orders_pb2
from confluent_kafka import Consumer, Producer
from confluent_kafka.schema_registry.protobuf import ProtobufDeserializer
from confluent_kafka.serialization import MessageField, SerializationContext
from prometheus_client import Counter, start_http_server

VERSION = os.environ["SCHEMA_VERSION"]
GROUP = os.environ["GROUP_ID"]

deser = ProtobufDeserializer(orders_pb2.OrderCreated, {"use.deprecated.format": False})

ok = Counter("consumer_orders_ok_total", "valid orders")
bad = Counter("consumer_orders_invalid_total", "failed validation", ["reason"])
revenue = Counter("consumer_revenue_cents_total", "revenue")

consumer = Consumer(
    {
        "bootstrap.servers": os.environ["BOOTSTRAP"],
        "group.id": GROUP,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
    }
)
dlq = Producer(
    {"bootstrap.servers": os.environ["BOOTSTRAP"]}
)  # For writing failed messages

consumer.subscribe(["orders"])

start_http_server(9091)


class ErrorMsg(Enum):
    CENTS_MISMATCH = "cents mismatch"
    EMPTY_ORDER = "empty order"
    DECODE_ERROR = "decode error"
    BAD_ITEM = "bad item"
    BAD_CURRENCY = "bad currency"
    BAD_TIMESTAMP = "bad timestamp"


for e in ErrorMsg:
    bad.labels(e.value)

VALID_CURRENCIES = ["CAD", "USD"]


def validate(order):
    problems = []
    if not order.items:
        problems.append(ErrorMsg.EMPTY_ORDER)
    if any(i.qty <= 0 or i.unit_price_cents <= 0 for i in order.items):
        problems.append(ErrorMsg.BAD_ITEM)
    expected = sum(i.qty * i.unit_price_cents for i in order.items)
    if order.total_cents != expected:
        problems.append(ErrorMsg.CENTS_MISMATCH)
    if hasattr(order, "currency") and order.currency not in VALID_CURRENCIES:
        problems.append(ErrorMsg.BAD_CURRENCY)
    now = int(time.time() * 1000)
    if not (now - 365 * 86400_000 < order.created_at_ms <= now + 60_000):
        problems.append(ErrorMsg.BAD_TIMESTAMP)
    return problems


def receive_orders():
    while True:
        m = consumer.poll(1.0)
        if m is None or m.error():
            continue

        try:
            order = deser(
                m.value(), SerializationContext(m.topic(), MessageField.VALUE)
            )
            reasons = validate(order)
        except Exception:
            order, reasons = None, [ErrorMsg.DECODE_ERROR]
        if len(reasons) > 0:
            for reason in reasons:
                bad.labels(str(reason)).inc()
            dlq.produce(
                "orders.dlq",
                key=m.key(),
                value=m.value(),
                headers=[
                    ("error", ",".join(r.value for r in reasons).encode()),
                    ("src-offset", str(m.offset()).encode()),
                ],
            )
            dlq.poll(0)
        else:
            ok.inc()
            revenue.inc(order.total_cents)
        consumer.commit(m)


def main():
    receive_orders()


if __name__ == "__main__":
    main()
