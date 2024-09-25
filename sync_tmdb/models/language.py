class Language:
    def __init__(self, name: str, code: str, tmdb_language: str):
        self.name = name
        self.code = code
        self.tmdb_language = tmdb_language

    def __str__(self):
        return f"{self.name} ({self.code} / {self.tmdb_language})"
