from typesense import Client
from prefect.blocks.system import Secret


class TypesenseClient:
    def __init__(self):
        self.host = Secret.load("typesense-host").get()
        self.port = Secret.load("typesense-port").get()
        self.protocol = Secret.load("typesense-protocol").get()
        self.api_key = Secret.load("typesense-api-key").get()

        self.client = Client(
            {
                "nodes": [
                    {
                        "host": self.host,
                        "port": self.port,
                        "protocol": self.protocol,
                    }
                ],
                "api_key": self.api_key,
                "connection_timeout_seconds": 10,
            }
        )

    def search_movies(self, title: str, per_page: int = 5) -> list[dict]:
        result = self.client.collections["movies"].documents.search(
            {
                "q": title,
                "query_by": "titles",
                "per_page": per_page,
            }
        )
        # Typesense's typo/prefix fallback always returns its "least bad" guess, even for a title
        # that plain doesn't exist in the catalog, so callers can't just trust "there were hits".
        # num_tokens_dropped tells us how many words of the query weren't found in this candidate
        # at all — attach that as a 0..1 "how much of the title actually matched" score (1.0 =
        # every word found) so matcher.py can fold it into a real confidence score with a
        # threshold, instead of an all-or-nothing filter here.
        candidates = []
        for hit in result.get("hits", []):
            info = hit.get("text_match_info", {})
            matched = info.get("tokens_matched", 0) or 0
            dropped = info.get("num_tokens_dropped", 0) or 0
            total = matched + dropped
            doc = hit["document"]
            doc["_title_score"] = (matched / total) if total else 0.0
            candidates.append(doc)
        return candidates
