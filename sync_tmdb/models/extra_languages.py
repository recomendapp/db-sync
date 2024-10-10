from .language import Language

class UnsupportedLanguage(Exception):
	pass

class ExtraLanguages:
    # Dictionary of supported extra languages
	supported_languages = {
        'fr-FR': Language(name='French', code='fr-FR', tmdb_language='fr-FR'),
		'es-ES': Language(name='Spanish', code='es-ES', tmdb_language='es-ES'),
        # Add more languages here
	}
     
	def __init__(self, languages: list = []):
		# Create array of extra languages
		self.languages = []
            
		# Add extra languages to array
		for language in languages:
			if language in self.supported_languages:
				self.languages.append(self.supported_languages[language])
			else:
				raise UnsupportedLanguage(f'Unsupported language: {language}')
	
	def __iter__(self):
		self._index = 0
		return self

	def __next__(self):
		if self._index < len(self.languages):
			language: Language = self.languages[self._index]
			self._index += 1
			return language
		else:
			raise StopIteration

	def __str__(self):
		return ', '.join([str(lang) for lang in self.languages])
	
	def __repr__(self):
		return f"ExtraLanguages({self.extra_languages!r})"