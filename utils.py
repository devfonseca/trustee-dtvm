from __future__ import annotations

import json
from datetime import date, datetime, timedelta
from decimal import Decimal, InvalidOperation
from typing import Any


def ensure_json_string(data: Any) -> str:
    return json.dumps(data, ensure_ascii=False, default=str)


def normalize_scalar(value: Any) -> Any:
    if value is None:
        return None

    # Alguns retornos podem vir em formato tipo XML convertido:
    # {"#text": "123"} ou {"@attributes": {...}, "#text": "abc"}
    if isinstance(value, dict):
        for key in ("#text", "text", "$text", "value", "Value"):
            if key in value:
                return normalize_scalar(value[key])

        # se for dict simples com 1 item, tenta desembrulhar
        if len(value) == 1:
            only_value = next(iter(value.values()))
            return normalize_scalar(only_value)

        return value

    if isinstance(value, list):
        if not value:
            return None
        return normalize_scalar(value[0])

    if isinstance(value, str):
        value = value.strip()
        return value or None

    return value


def parse_decimal(value: Any) -> Decimal | None:
    value = normalize_scalar(value)
    if value is None:
        return None

    if isinstance(value, (int, float, Decimal)):
        try:
            return Decimal(str(value))
        except InvalidOperation:
            return None

    if not isinstance(value, str):
        return None

    cleaned = value.strip()
    if not cleaned:
        return None

    cleaned = (
        cleaned.replace("R$", "")
        .replace("%", "")
        .replace(" ", "")
        .replace("\xa0", "")
    )

    # padrão BR
    if "," in cleaned:
        cleaned = cleaned.replace(".", "").replace(",", ".")

    try:
        return Decimal(cleaned)
    except InvalidOperation:
        return None


def parse_date(value: Any) -> str | None:
    value = normalize_scalar(value)
    if value is None:
        return None

    if isinstance(value, date):
        return value.isoformat()

    if not isinstance(value, str):
        return None

    raw = value.strip()
    if not raw:
        return None

    formats = [
        "%Y-%m-%d",
        "%Y/%m/%d",
        "%d/%m/%Y",
        "%d-%m-%Y",
        "%Y-%m-%d %H:%M:%S",
        "%Y/%m/%d %H:%M:%S",
        "%d/%m/%Y %H:%M:%S",
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%dT%H:%M:%S.%f",
        "%Y/%m/%dT%H:%M:%S",
    ]

    for fmt in formats:
        try:
            return datetime.strptime(raw, fmt).date().isoformat()
        except ValueError:
            continue

    return None


def today_iso() -> str:
    return date.today().isoformat()


def days_ago_iso(days: int) -> str:
    return (date.today() - timedelta(days=days)).isoformat()


def first_existing(data: dict, *keys: str) -> Any:
    for key in keys:
        if key in data:
            value = normalize_scalar(data[key])
            if value not in (None, "", [], {}, ()):
                return value
    return None