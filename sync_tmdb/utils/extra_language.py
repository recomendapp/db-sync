class ExtraLanguage:
    def __init__(self, name: str, code: str, tmdb_language: str):
        self.name = name
        self.code = code
        self.tmdb_language = tmdb_language

    def __str__(self):
        return f"{self.name} ({self.code} / {self.tmdb_language})"

class UnsupportedLanguage(Exception):
	pass

class ExtraLanguages:
    # Dictionary of supported extra languages
	supported_languages = {
        'fr': ExtraLanguage(name='French', code='fr', tmdb_language='fr-FR'),
        # Add more languages here
	}
     
	def __init__(self, languages: list = []):
		# Create array of extra languages
		self.extra_languages = []
            
		# Add extra languages to array
		for language in languages:
			if language in self.supported_languages:
				self.extra_languages.append(self.supported_languages[language])
			else:
				raise UnsupportedLanguage(f'Unsupported language: {language}')
    
	def __str__(self):
		return ', '.join([str(lang) for lang in self.extra_languages])
	
	def __repr__(self):
		return f"ExtraLanguages({self.extra_languages!r})"