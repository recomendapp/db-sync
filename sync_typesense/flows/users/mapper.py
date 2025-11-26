class Mapper:
    @staticmethod
    def user(row: tuple) -> dict:
        (
            user_id,
            username,
            full_name,
            followers_count
        ) = row

        return {
            "id": str(user_id),
            "username": username or "",
            "full_name": full_name or "",
            "followers_count": int(followers_count or 0),
        }
