"""
Standalone script to run a full (unbounded) sync of Open Library authors only.

Not part of the sync_open_library package: this is a manual testing helper. It caches
the downloaded dumps at a fixed path under .tmp/ and reuses them on a re-run instead of
re-downloading — the packaged flows always clean up after themselves when they do their
own download, but here we download ourselves and pass the path in, so that cleanup
never kicks in (see sync_open_library_author's `downloaded_dump_here` check).

Usage: cd db-sync && .venv/bin/python run_authors_full.py

Estimated time: ~70-90 minutes for the full ~15M authors (no max_records this time,
so pruning runs for real too).
"""
import os
import time
from datetime import date

import requests

from sync_open_library.models.config import DEFAULT_DUMP_URLS
from sync_open_library.flows.author.sync_open_library_author import sync_open_library_author

TMP_DIR = ".tmp"
AUTHOR_DUMP_PATH = os.path.join(TMP_DIR, "ol_dump_authors_cached.gz")
REDIRECTS_DUMP_PATH = os.path.join(TMP_DIR, "ol_dump_redirects_cached.gz")

def download_if_missing(url: str, path: str):
	if os.path.exists(path):
		print(f"Reusing cached {path} ({os.path.getsize(path) / 1024 / 1024:.1f} MB)")
		return
	os.makedirs(TMP_DIR, exist_ok=True)
	print(f"Downloading {url} -> {path} ...")
	with requests.get(url, stream=True, timeout=(10, 300)) as response:
		response.raise_for_status()
		with open(path, "wb") as f:
			for chunk in response.iter_content(chunk_size=8 * 1024 * 1024):
				if chunk:
					f.write(chunk)
	print(f"Downloaded {os.path.getsize(path) / 1024 / 1024:.1f} MB")

if __name__ == "__main__":
	download_if_missing(DEFAULT_DUMP_URLS["redirects"], REDIRECTS_DUMP_PATH)
	download_if_missing(DEFAULT_DUMP_URLS["author"], AUTHOR_DUMP_PATH)

	start = time.time()
	sync_open_library_author(date=date.today(), dump_path=AUTHOR_DUMP_PATH, redirects_path=REDIRECTS_DUMP_PATH)
	elapsed = time.time() - start
	print(f"TOTAL WALL TIME: {elapsed:.1f}s ({elapsed / 60:.1f} min)")
	print(f"Dump files kept at {AUTHOR_DUMP_PATH} and {REDIRECTS_DUMP_PATH} for reuse — delete .tmp/ manually when done.")
