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
            visibility,
            owner_id,
            member_ids
        ) = row

        doc = {
            "id": str(playlist_id),
            "title": title or "",
            "description": description or "",
            "likes_count": int(likes_count or 0),
            "items_count": int(items_count or 0),
            "created_at": int(created_ts),
            "updated_at": int(updated_ts),
            "visibility": visibility or "public",
            "owner_id": str(owner_id),
            "member_ids": [str(m) for m in (member_ids or [])],
        }

        return doc
