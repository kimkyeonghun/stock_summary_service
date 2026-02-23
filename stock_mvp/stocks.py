from __future__ import annotations

from stock_mvp.models import Stock


# Korean seed universe for MVP bootstrap.
DEFAULT_STOCKS: list[Stock] = [
    Stock(
        code="005930",
        name="Samsung Electronics",
        queries=["\uc0bc\uc131\uc804\uc790", "005930"],
        market="KR",
        exchange="KRX",
        currency="KRW",
        rank=1,
    ),
    Stock(
        code="000660",
        name="SK Hynix",
        queries=["SK\ud558\uc774\ub2c9\uc2a4", "000660"],
        market="KR",
        exchange="KRX",
        currency="KRW",
        rank=2,
    ),
    Stock(
        code="373220",
        name="LG Energy Solution",
        queries=["LG\uc5d0\ub108\uc9c0\uc194\ub8e8\uc158", "373220"],
        market="KR",
        exchange="KRX",
        currency="KRW",
        rank=3,
    ),
    Stock(
        code="005380",
        name="Hyundai Motor",
        queries=["\ud604\ub300\ucc28", "005380"],
        market="KR",
        exchange="KRX",
        currency="KRW",
        rank=4,
    ),
    Stock(
        code="000270",
        name="Kia",
        queries=["\uae30\uc544", "000270"],
        market="KR",
        exchange="KRX",
        currency="KRW",
        rank=5,
    ),
    Stock(
        code="035420",
        name="NAVER",
        queries=["NAVER", "\ub124\uc774\ubc84", "035420"],
        market="KR",
        exchange="KRX",
        currency="KRW",
        rank=6,
    ),
    Stock(
        code="035720",
        name="Kakao",
        queries=["\uce74\uce74\uc624", "035720"],
        market="KR",
        exchange="KRX",
        currency="KRW",
        rank=7,
    ),
    Stock(
        code="068270",
        name="Celltrion",
        queries=["\uc140\ud2b8\ub9ac\uc628", "068270"],
        market="KR",
        exchange="KRX",
        currency="KRW",
        rank=8,
    ),
    Stock(
        code="005490",
        name="POSCO Holdings",
        queries=["POSCO\ud640\ub529\uc2a4", "\ud3ec\uc2a4\ucf54\ud640\ub529\uc2a4", "005490"],
        market="KR",
        exchange="KRX",
        currency="KRW",
        rank=9,
    ),
    Stock(
        code="207940",
        name="Samsung Biologics",
        queries=["\uc0bc\uc131\ubc14\uc774\uc624\ub85c\uc9c1\uc2a4", "207940"],
        market="KR",
        exchange="KRX",
        currency="KRW",
        rank=10,
    ),
]

