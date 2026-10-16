# protobs/__main__.py
import signal
import threading

from .config import Config
from .sink import PostgresSink
from .tap import Tap


def main() -> None:
    cfg = Config()
    sink = PostgresSink(cfg.database_url, cfg.slow_write_seconds)

    stop = threading.Event()
    signal.signal(signal.SIGTERM, lambda *_: stop.set())
    signal.signal(signal.SIGINT, lambda *_: stop.set())
    tap = Tap(sink, cfg)
    tap.run(stop)


if __name__ == "__main__":
    main()
