class Mapper:
	@staticmethod
	def movie(row: tuple) -> dict:
		(
			movie_id,
			original_title,
			popularity,
			genre_ids,
			runtime,
			release_ts,
			titles
		) = row

		# Normalize title list
		title_set = set()
		if titles:
			for t in titles:
				if t and t.strip():
					title_set.add(t.strip())

		if original_title and original_title.strip():
			title_set.add(original_title.strip())

		# Build Typesense document
		doc = {
			"id": str(movie_id),
			"original_title": original_title or "",
			"titles": list(title_set),
			"popularity": float(popularity or 0),
			"genre_ids": [int(g) for g in (genre_ids or [])],
		}

		if runtime is not None:
			doc["runtime"] = int(runtime)

		if release_ts is not None:
			doc["release_date"] = int(release_ts)

		return doc