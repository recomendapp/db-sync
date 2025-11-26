class Mapper:
    @staticmethod
    def playlist(row: tuple) -> dict:
        (
            playlist_id,
            title,
            description,
            likes_count,
            items_count,
            created_ts,
            updated_ts,
            is_private,
            owner_id,
            guest_ids,
            type_
        ) = row

        doc = {
            "id": str(playlist_id),
            "title": title or "",
            "description": description or "",
            "likes_count": int(likes_count or 0),
            "items_count": int(items_count or 0),
            "created_at": int(created_ts),
            "updated_at": int(updated_ts),
            "is_private": bool(is_private),
            "owner_id": str(owner_id),
            "guest_ids": [str(g) for g in (guest_ids or [])],
            "type": type_ or "",
        }

        return doc
