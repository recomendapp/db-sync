class Mapper:
    @staticmethod
    def user(row: tuple) -> dict:
        (
            user_id,
            username,
            name,
            followers_count
        ) = row

        return {
            "id": str(user_id),
            "username": username or "",
            "name": name or "",
            "followers_count": int(followers_count or 0),
        }
