import os


class Settings:
    def __init__(self) -> None:
        self.vendor_api_base = os.environ.get("VENDOR_API_BASE", "http://vendor-api:8080")
        self.kafka_bootstrap = os.environ.get("KAFKA_BOOTSTRAP", "kafka:9093")
        self.network_events_topic = os.environ.get("NETWORK_EVENTS_TOPIC", "network-events")
        self.network_incidents_topic = os.environ.get("NETWORK_INCIDENTS_TOPIC", "network-incidents")


SETTINGS = Settings()
