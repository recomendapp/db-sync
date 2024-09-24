class Language:
	def __init__(self, iso_639_1: str, name_in_native_language: str):
		self.iso_639_1 = iso_639_1
		self.name_in_native_language = name_in_native_language

class LanguageTranslation:
	def __init__(self, iso_639_1: str, language: str, name: str):
		self.iso_639_1 = iso_639_1
		self.language = language
		self.name = name