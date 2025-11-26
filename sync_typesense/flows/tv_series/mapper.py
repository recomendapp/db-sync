class Mapper:
    @staticmethod
    def tv_series(row: tuple) -> dict:
        (
            serie_id,
            original_name,
            popularity,
            genre_ids,
            num_episodes,
            num_seasons,
            vote_average,
            vote_count,
            status,
            type_,
            first_air_ts,
            last_air_ts,
            names,
        ) = row

        # Normalize names
        name_set = set()

        if names:
            for n in names:
                if n and n.strip():
                    name_set.add(n.strip())

        if original_name and original_name.strip():
            name_set.add(original_name.strip())

		# Build Typesense document
        doc = {
            "id": str(serie_id),
            "original_name": original_name or "",
            "names": list(name_set),
            "popularity": float(popularity or 0),
            "genre_ids": [int(g) for g in (genre_ids or [])],
        }

        if num_episodes is not None:
            doc["number_of_episodes"] = int(num_episodes)

        if num_seasons is not None:
            doc["number_of_seasons"] = int(num_seasons)

        if vote_average is not None:
            doc["vote_average"] = float(vote_average)

        if vote_count is not None:
            doc["vote_count"] = int(vote_count)

        if status:
            doc["status"] = status

        if type_:
            doc["type"] = type_

        if first_air_ts is not None:
            doc["first_air_date"] = int(first_air_ts)

        if last_air_ts is not None:
            doc["last_air_date"] = int(last_air_ts)

        return doc
