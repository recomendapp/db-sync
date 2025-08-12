from .sync_log import SyncLog

class SyncLogsManager:
	def __init__(self, config) -> None:
		from .config import Config
		self.config: Config = config
		self.db_client = config.db_client
		self.table = self.config.config.get("logs", {}).get("table", "sync_logs")
		self.type: str = None
		self.current_log: SyncLog = None
		self.last_success_log: SyncLog = None
		self.logger = self.config.logger
	
	def get_last_success_log(self, type: str, status: str = "success") -> SyncLog:
		"""
		Get the last success log for the actual type
		"""
		conn = self.db_client.get_connection()
		try:
			with conn.cursor() as cursor:
				cursor.execute(f"SELECT id, type, status, date FROM {self.table} WHERE type = %s AND status = %s ORDER BY date DESC LIMIT 1", (type, status))
				row = cursor.fetchone()
				if row:
					return SyncLog(id=row[0], type=row[1], status=row[2], date=row[3])
				return None
		except Exception as e:
			raise ValueError(f"Failed to get last success log: {e}")
		finally:
			self.db_client.return_connection(conn)
			
	def create_log(self, type: str, status: str = "initialized") -> SyncLog:
		"""
		Create a new log for the actual date and type
		"""
		conn = self.db_client.get_connection()
		try:
			with conn.cursor() as cursor:
				cursor.execute(f"INSERT INTO {self.table} (type, status, date) VALUES (%s, %s, %s) RETURNING id", (type, status, self.config.date))
				row = cursor.fetchone()
				conn.commit()
				return SyncLog(id=row[0], type=type, status=status, date=self.config.date)
		except Exception as e:
			raise ValueError(f"Failed to create log: {e}")
		finally:
			self.db_client.return_connection(conn)
			
	def update_log(self, status: str) -> None:
		"""
		Update the status of the current log
		"""
		if self.type is None:
			return None
		elif self.current_log is None:
			self.init(type=self.type, status=status)
		else:
			conn = self.db_client.get_connection()
			try:
				with conn.cursor() as cursor:
					cursor.execute(
						f"""
						UPDATE {self.table}
						SET status = %s, updated_at = NOW()
						WHERE id = %s
						""",
						(status, self.current_log.id)
					)
					conn.commit()
					self.logger.info(f"Log {self.current_log.id} updated to {status}")
					self.current_log.status = status
			except Exception as e:
				raise ValueError(f"Failed to update log: {e}")
			finally:
				self.db_client.return_connection(conn)
	
	def delete_log(self, id: int) -> None:
		"""
		Delete a log by its id
		"""
		conn = self.db_client.get_connection()
		try:
			with conn.cursor() as cursor:
				cursor.execute(f"DELETE FROM {self.table} WHERE id = %s", (id,))
				conn.commit()
		except Exception as e:
			raise ValueError(f"Failed to delete log: {e}")
		finally:
			self.db_client.return_connection(conn)

	# ---------------------------------- Status ---------------------------------- #
	def init(self, type: str, status: str = "initialized") -> None:
		"""
		Initializes the log
		"""
		self.type = type
		self.current_log = self.create_log(type=type, status=status)
		self.last_success_log = self.get_last_success_log(type=type)

	def fetching_data(self) -> None:
		"""
		Update the status of the current log to fetching_data
		"""
		self.update_log("fetching_data")

	def data_fetched(self) -> None:
		"""
		Update the status of the current log to data_fetched
		"""
		self.update_log("data_fetched")
	
	def syncing_to_db(self) -> None:
		"""
		Update the status of the current log to syncing_to_db
		"""
		self.update_log("syncing_to_db")
	
	def updating_popularity(self) -> None:
		"""
		Update the status of the current log to updating_popularity
		"""
		self.update_log("updating_popularity")

	def failed(self) -> None:
		"""
		Update the status of the current log to failed
		"""
		self.update_log("failed")

	def success(self) -> None:
		"""
		Update the status of the current log to success
		"""
		self.update_log("success")

	# ---------------------------------------------------------------------------- #