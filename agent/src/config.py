from pydantic_settings import BaseSettings, SettingsConfigDict


class Config(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="CAPTURE_")

    brokers: str = "localhost:19092"
    bootstrap: str = "localhost:19092"
    topics: list[str] = ["orders"]
    group_id: str = "capture-agent"
    source: str = "local"
    batch_size: int = 500
    max_batch_seconds: float = 2.0
    slow_write_seconds: float = 1.5
    pause_cooldown_seconds: float = 5.0
    database_url: str
