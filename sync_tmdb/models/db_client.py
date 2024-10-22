import psycopg2.pool
from prefect.blocks.system import Secret

class DBClient:
	def __init__(self):
		self.connection_string = self._get_postgres_connection_string("postgres-connection-string")
		self.connection_pool = psycopg2.pool.SimpleConnectionPool(1, 20, self.connection_string)
	
	def _get_postgres_connection_string(self, secret_name: str) -> str:
		try:
			return Secret.load(secret_name).get()
		except Exception as e:
			raise ValueError(f"Postgres connection string not found: {e}")
		
	def get_connection(self):
		return self.connection_pool.getconn()
	
	def return_connection(self, conn):
		self.connection_pool.putconn(conn)

	def close_connection(self):
		self.connection_pool.closeall()

	def get_table(self, table_name: str, columns: list) -> list:
		conn = self.get_connection()
		try:
			with conn.cursor() as cursor:
				cursor.execute(f"SELECT {', '.join(columns)} FROM {table_name}")
				return cursor.fetchall()
		except Exception as e:
			raise ValueError(f"Failed to get table {table_name}: {e}")
		finally:
			self.return_connection(conn)
