from sync_tmdb.models.extra_language import ExtraLanguages

class Config:
	def __init__(self, extra_languages: list):
		# Supported extra languages (default is English)
		self.extra_languages = ExtraLanguages(extra_languages)


