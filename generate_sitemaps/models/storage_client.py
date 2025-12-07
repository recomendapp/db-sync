from supabase import create_client, Client
from prefect.blocks.system import Secret

class StorageClient:
    def __init__(self):
        url = self._get_secret("supabase-url")
        key = self._get_secret("supabase-service-role-key")
        self.client: Client = create_client(url, key)

    def _get_secret(self, secret_name: str) -> str:
        try:
            return Secret.load(secret_name).get()
        except Exception as e:
            raise ValueError(f"Supabase secret '{secret_name}' not found: {e}")

    def upload(self, path: str, content: bytes):
        self.client.storage.from_("sitemaps").upload(
            path=path,
            file=content,
            file_options={"cache-control": "3600", "upsert": "true", "content-type": "application/xml"},
        )
