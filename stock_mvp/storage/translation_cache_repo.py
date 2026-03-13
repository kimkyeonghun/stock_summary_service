from __future__ import annotations

from typing import Any

from stock_mvp.utils import now_utc_iso


def get_translation(conn, *, source_hash: str) -> dict[str, Any] | None:
    row = conn.execute(
        """
        SELECT source_hash, src_text, ko_text, model, updated_at
        FROM translation_cache
        WHERE source_hash = ?
        """,
        (source_hash,),
    ).fetchone()
    if row is None:
        return None
    return {
        "source_hash": str(row["source_hash"]),
        "src_text": str(row["src_text"] or ""),
        "ko_text": str(row["ko_text"] or ""),
        "model": str(row["model"] or ""),
        "updated_at": str(row["updated_at"] or ""),
    }


def upsert_translation(
    conn,
    *,
    source_hash: str,
    src_text: str,
    ko_text: str,
    model: str,
    commit: bool = False,
) -> None:
    conn.execute(
        """
        INSERT INTO translation_cache(source_hash, src_text, ko_text, model, updated_at)
        VALUES (?, ?, ?, ?, ?)
        ON CONFLICT(source_hash) DO UPDATE SET
          src_text=excluded.src_text,
          ko_text=excluded.ko_text,
          model=excluded.model,
          updated_at=excluded.updated_at
        """,
        (source_hash, src_text, ko_text, model, now_utc_iso()),
    )
    if commit:
        conn.commit()

