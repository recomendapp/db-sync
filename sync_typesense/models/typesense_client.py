from typesense import Client
from prefect.blocks.system import Secret

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
			"connection_timeout_seconds": 120
		})

	def upsert_documents(self, collection: str, documents: list[dict]):
		return self.client.collections[collection].documents.import_(documents, {'action': 'upsert'})

	def delete_documents(self, collection: str, ids: list[int]):
		if not ids:
			return

		ids_str = ",".join(map(str, ids))
		filter_query = f"id:=[{ids_str}]"

		try:
			return self.client.collections[collection].documents.delete({
				"filter_by": filter_query
			})
		except Exception as e:
			raise ValueError(f"Batch delete failed: {e}")
