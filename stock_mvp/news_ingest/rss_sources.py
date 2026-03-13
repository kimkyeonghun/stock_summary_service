from __future__ import annotations

DEFAULT_KR_RSS_SOURCES: list[dict[str, object]] = [
    {
        "source_name": "hankyung_all",
        "feed_url": "https://www.hankyung.com/feed/all-news",
        "category": "news",
        "polling_minutes": 20,
    },
    {
        "source_name": "hankyung_finance",
        "feed_url": "https://www.hankyung.com/feed/finance",
        "category": "finance",
        "polling_minutes": 15,
    },
    {
        "source_name": "hankyung_economy",
        "feed_url": "https://www.hankyung.com/feed/economy",
        "category": "economy",
        "polling_minutes": 20,
    },
    {
        "source_name": "mk_headline",
        "feed_url": "https://www.mk.co.kr/rss/30000001/",
        "category": "headline",
        "polling_minutes": 20,
    },
    {
        "source_name": "mk_economy",
        "feed_url": "https://www.mk.co.kr/rss/30100041/",
        "category": "economy",
        "polling_minutes": 20,
    },
    {
        "source_name": "mk_stock",
        "feed_url": "https://www.mk.co.kr/rss/50200011/",
        "category": "stock",
        "polling_minutes": 15,
    },
]

