from typesense import Client
from prefect.blocks.system import Secret
from typing import Iterator

# A delete carries every id in its filter, and the API offers only the query-string
# form: httpx refuses a URL component past 65_536 characters, and whatever fronts
# Typesense usually caps the request line far lower. So the ids are spent a few
# kilobytes at a time instead of in one filter.
TYPESENSE_DELETE_FILTER_MAX_CHARS = 4096


def _chunk_ids(ids: list[int], max_chars: int) -> Iterator[list[str]]:
	"""Group ids into chunks whose comma-joined length stays under max_chars."""
	chunk: list[str] = []
	length = 0
	for value in ids:
		text = str(value)
		if chunk and length + 1 + len(text) > max_chars:
			yield chunk
			chunk, length = [], 0
		length += len(text) + (1 if chunk else 0)
		chunk.append(text)
	if chunk:
		yield chunk

class TypesenseClient:
	def __init__(self):
		self.host = Secret.load("typesense-host").get()
		self.port = Secret.load("typesense-port").get()
		self.protocol = Secret.load("typesense-protocol").get()
		self.api_key = Secret.load("typesense-api-key").get()

		self.client = Client({
			"nodes": [
				{
					"host": self.host,
					"port": self.port,
					"protocol": self.protocol
				}
			],
			"api_key": self.api_key,
			"connection_timeout_seconds": 2
		})

	def upsert_documents(self, collection: str, documents: list[dict]):
		return self.client.collections[collection].documents.import_(documents, {'action': 'upsert'})

	def delete_documents(self, collection: str, ids: list[int]):
		if not ids:
			return

		deleted = 0
		try:
			for chunk in _chunk_ids(ids, TYPESENSE_DELETE_FILTER_MAX_CHARS):
				response = self.client.collections[collection].documents.delete({
					"filter_by": f"id:=[{','.join(chunk)}]"
				})
				deleted += response.get("num_deleted", 0) if isinstance(response, dict) else 0
		except Exception as e:
			raise ValueError(f"Batch delete failed: {e}")

		return {"num_deleted": deleted}
