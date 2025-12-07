import gzip
from typing import List, Dict, TypedDict, Optional

class Alternate(TypedDict, total=False):
    languages: Dict[str, str]

class SitemapEntry(TypedDict, total=False):
    url: str
    lastModified: Optional[str]
    changeFrequency: Optional[str]
    priority: Optional[float]
    alternates: Optional[Alternate]

def build_sitemap_index(sitemaps: List[str]) -> str:
    xml = '<?xml version="1.0" encoding="UTF-8"?>'
    xml += '<sitemapindex xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">'
    for sitemap_url in sitemaps:
        xml += "<sitemap>"
        xml += f"<loc>{sitemap_url}</loc>"
        xml += "</sitemap>"
    xml += "</sitemapindex>"
    return xml

def build_sitemap(urls: List[SitemapEntry]) -> str:
    xml = '<?xml version="1.0" encoding="UTF-8"?>'
    xml += '<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9" xmlns:xhtml="http://www.w3.org/1999/xhtml">'
    for url in urls:
        xml += "<url>"
        xml += f"<loc>{url['url']}</loc>"
        if 'lastModified' in url and url['lastModified']:
            xml += f"<lastmod>{url['lastModified']}</lastmod>"
        if 'changeFrequency' in url:
            xml += f"<changefreq>{url['changeFrequency']}</changefreq>"
        if 'priority' in url:
            xml += f"<priority>{url['priority']}</priority>"
        if 'alternates' in url and 'languages' in url['alternates']:
            for lang, href in url['alternates']['languages'].items():
                xml += f'<xhtml:link rel="alternate" hreflang="{lang}" href="{href}" />'
        xml += "</url>"
    xml += "</urlset>"
    return xml

def gzip_encode(text: str) -> bytes:
    return gzip.compress(text.encode('utf-8'))
