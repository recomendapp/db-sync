from datetime import date

class SyncLog:
	def __init__(self, id: int, type: str, status: str, date: date):
		self.id = id
		self.type = type
		self.status = status
		self.date = date