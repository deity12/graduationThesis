"""Prompt helpers for Stage 4 spillover extraction."""

from __future__ import annotations

from typing import Any


def _coalesce_article_text(news_row: dict[str, Any], max_chars: int = 4000) -> str:
    parts: list[str] = []
    title = str(news_row.get("title", "") or "").strip()
    if title:
        parts.append(f"Title: {title}")

    body = str(news_row.get("body", "") or "").strip()
    if body:
        parts.append(f"Body: {body}")
    else:
        for field in ("summary_textrank", "summary_lsa", "summary_luhn", "summary_lexrank"):
            summary = str(news_row.get(field, "") or "").strip()
            if summary:
                parts.append(f"Summary: {summary}")
                break

    article_text = "\n".join(parts).strip()
    return article_text[:max_chars]


def build_stage4_messages(news_row: dict[str, Any], event_types: list[str]) -> list[dict[str, str]]:
    """Create the system and user messages for one Stage 4 extraction request."""
    allowed_event_types = ", ".join(event_types)
    article_text = _coalesce_article_text(news_row)
    source_hint = str(news_row.get("source_company_name", "") or news_row.get("source_ticker", "") or "").strip()
    published_date = str(news_row.get("published_date", "") or "").strip()

    system_prompt = (
        "You extract spillover events from one financial news article.\n\n"
        "Return exactly one JSON object with only these three top-level keys:\n"
        '- source: string (the primary company or issuer that anchors the event; use "" if unknown)\n'
        "- targets: array of strings (companies or issuers explicitly described in the article as affected by the event; "
        "use [] if none)\n"
        "- event_type: string from the allowed enum below\n\n"
        "Event type rules:\n"
        "- Use the most specific matching category when the article clearly describes an event.\n"
        "- Use 'other' when the article clearly contains an event but none of the specific allowed categories fits.\n"
        "- Use \"\" only when the event type cannot be determined from the article.\n\n"
        "Extraction rules:\n"
        "- source should be the primary event-carrying entity in the article.\n"
        "- targets should only include entities explicitly named in the article as being affected by the event.\n"
        "- Do not infer targets from general market knowledge, peer similarity, sectors, indices, countries, or macro "
        "assets unless they are explicitly named as affected parties in the article.\n"
        "- If no affected entity is mentioned, return targets as [].\n\n"
        "Do not add rationale, confidence, polarity, strength, markdown, or any extra keys.\n"
        "Do not wrap the JSON in code blocks or add any text outside the JSON object."
    )

    user_prompt = (
        f"Published date: {published_date}\n"
        f"Source hint (weak): {source_hint or 'unknown'}\n"
        f"Allowed event types: {allowed_event_types}\n\n"
        "The source hint above is only a weak suggestion. Use it if it matches the article content, but rely on the "
        "article itself when the hint is inconsistent or missing.\n\n"
        "Extract from the article below:\n"
        "- source: the primary company or issuer anchoring the event\n"
        "- targets: companies or issuers explicitly described in the article as affected by the event\n"
        "- event_type: one value from the allowed list above\n\n"
        f"{article_text}"
    )

    return [
        {"role": "system", "content": system_prompt},
        {"role": "user", "content": user_prompt},
    ]
