def strip_ol_key(key: str | None) -> str | None:
	"""
	Open Library keys are always the last path segment of a full OL path:
	"/authors/OL23919A" -> "OL23919A", "/works/OL27448W" -> "OL27448W",
	"/books/OL7353617M" -> "OL7353617M".

	The dump's own key column *and* every cross-reference embedded in a record's JSON
	(work.authors[].author.key, edition.works[].key...) use this full-path form, but
	our own tables store the bare id — this must be applied consistently everywhere
	a key crosses that boundary.
	"""
	if not key:
		return None
	return key.rstrip('/').rsplit('/', 1)[-1]
