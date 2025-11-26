import psycopg2
from prefect.blocks.system import Secret
from contextlib import contextmanager

class DBClient:
	def __init__(self):
		self.connection_string = self._get_postgres_connection_string("postgres-connection-string")
	
	def _get_postgres_connection_string(self, secret_name: str) -> str:
		try:
			return Secret.load(secret_name).get()
		except Exception as e:
			raise ValueError(f"Postgres connection string not found: {e}")
		
	def get_connection(self):
		return psycopg2.connect(self.connection_string)
	
	def return_connection(self, conn):
		return conn.close()

	def close_connection(self):
		self.connection_pool.closeall()
	
	@contextmanager
	def connection(self):
		conn = self.get_connection()
		try:
			yield conn
		finally:
			self.return_connection(conn)

