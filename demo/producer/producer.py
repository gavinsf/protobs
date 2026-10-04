import os
import random
import time
import uuid

import orders_pb2
from confluent_kafka import Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.protobuf import ProtobufSerializer
from confluent_kafka.serialization import MessageField, SerializationContext

VERSION = os.environ["SCHEMA_VERSION"]
TOPIC = "orders"
RATE = float(os.getenv("RATE_PER_SEC", "20"))

rng = random.Random(int(os.getenv("SEED", "1")))
SKUS = [(f"SKU-{i:04d}", rng.randint(299, 19999)) for i in range(200)]

registry = SchemaRegistryClient({"url": os.environ["REGISTRY_URL"]})
serializer = ProtobufSerializer(
    orders_pb2.OrderCreated, registry, {"use.deprecated.format": False}
)

producer = Producer({"bootstrap.servers": os.environ["BOOTSTRAP"]})


def make_order():
    order = orders_pb2.OrderCreated()
    order.order_id = str(uuid.UUID(int=rng.getrandbits(128)))
    order.customer_id = f"C-{rng.randint(1, 5000)}"
    total = 0
    for sku, price in rng.sample(SKUS, k=rng.choice([1, 1, 1, 2, 3, 4, 8])):
        li = order.items.add()  # Repeated field LineItem
        li.sku, li.qty, li.unit_price_cents = sku, rng.randint(1, 5), price
        total += li.qty * price

    order.total_cents = total
    set_version_fields(order)
    order.created_at_ms = int(time.time() * 1000)
    return order


def set_version_fields(order):
    if hasattr(order, "currency") and VERSION in ("v1", "v2", "v3"):
        order.currency = rng.choice(["CAD", "CAD", "CAD", "USD"])

    if VERSION == "v2" and rng.random() < 0.2:
        order.discount_code = rng.choice(["SPRING10", "VIP20", "FREESHIP"])

    if VERSION == "v3b" and rng.random() < 0.3:
        order.promo_code = rng.choice(["SPRING10", "VIP20"])


def send_orders():
    try:
        while True:
            order = make_order()
            producer.produce(
                TOPIC,
                key=order.customer_id.encode(),
                value=serializer(
                    order,
                    SerializationContext(TOPIC, MessageField.VALUE),
                ),
                headers=[("schema-version", VERSION.encode())],
            )

            producer.poll(0)
            time.sleep(1 / RATE)
    except KeyboardInterrupt:
        pass
    finally:
        producer.flush(10)


def main():
    send_orders()


if __name__ == "__main__":
    main()
