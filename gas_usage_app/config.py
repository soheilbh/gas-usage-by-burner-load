"""
InfluxDB and app configuration.
All connection settings (host, port, database, user, password) are configurable
via environment variables or Streamlit session state / UI inputs.
"""
import os
from dataclasses import dataclass
from typing import Optional


@dataclass
class InfluxConfig:
    """InfluxDB connection parameters."""
    host: str
    port: str
    database: str
    retention_policy: str = "autogen"
    username: Optional[str] = None
    password: Optional[str] = None
    ssl: bool = False

    @classmethod
    def from_env(cls) -> "InfluxConfig":
        return cls(
            host=os.getenv("INFLUXDB_HOST", "localhost"),
            port=os.getenv("INFLUXDB_PORT", "8087"),
            database=os.getenv("INFLUXDB_DATABASE", "farmsum_db"),
            retention_policy=os.getenv("INFLUXDB_RETENTION_POLICY", "autogen"),
            username=os.getenv("INFLUXDB_USERNAME") or None,
            password=os.getenv("INFLUXDB_PASSWORD") or None,
            ssl=os.getenv("INFLUXDB_SSL", "false").lower() in ("true", "1", "yes"),
        )

    def base_url(self) -> str:
        protocol = "https" if self.ssl else "http"
        return f"{protocol}://{self.host}:{self.port}"
