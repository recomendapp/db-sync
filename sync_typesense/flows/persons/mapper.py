class Mapper:
    @staticmethod
    def person(row: tuple) -> dict:
        (
            person_id,
            name,
            popularity,
            known_for,
            also_known_as,
        ) = row

        name_set = set()

        if also_known_as:
            for t in also_known_as:
                if t and t.strip():
                    name_set.add(t.strip())

        if name and name.strip():
            name_set.add(name.strip())

        doc = {
            "id": str(person_id),
            "name": name or "",
            "also_known_as": list(name_set),
            "popularity": float(popularity or 0),
        }

        if known_for:
            doc["known_for_department"] = known_for

        return doc
