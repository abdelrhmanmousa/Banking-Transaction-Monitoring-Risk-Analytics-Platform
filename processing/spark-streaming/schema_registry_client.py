# schema_registry_client.py
import requests

class SchemaRegistryClient:
    def __init__(self, base_url: str):
        self.base_url = base_url.rstrip("/")

    def latest_schema(self, topic: str) -> str:
        url = f"{self.base_url}/subjects/{topic}-value/versions/latest"
        r = requests.get(url, timeout=10)
        r.raise_for_status()
        return r.json()["schema"]
