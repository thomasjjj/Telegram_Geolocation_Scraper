"""Common input validation helpers for interactive prompts."""

from __future__ import annotations

from datetime import datetime
from typing import Callable

Validator = Callable[[str], bool]


def validate_date(date_str: str) -> bool:
    """Return ``True`` if *date_str* matches the YYYY-MM-DD format."""

    try:
        datetime.strptime(date_str, "%Y-%m-%d")
    except ValueError:
        return False
    return True


def validate_positive_int(value: str) -> bool:
    """Return ``True`` when *value* is a positive integer string."""

    return value.isdigit() and int(value) > 0


def validate_non_empty(value: str) -> bool:
    """Return ``True`` when *value* contains at least one non-space character."""

    return bool(value.strip())


def prompt_validated(
    message: str,
    validator: Validator,
    *,
    error_msg: str = "Invalid input, try again",
    allow_empty: bool = False,
    empty_value: str = "",
) -> str:
    """Prompt the user until *validator* returns ``True``.

    Parameters
    ----------
    message:
        Text shown to the user prior to reading input.
    validator:
        Callable that receives the raw input string and returns ``True`` when
        the value is acceptable.
    error_msg:
        Message displayed whenever the validator rejects the input.
    allow_empty:
        When set to ``True`` an empty response is accepted immediately and
        :data:`empty_value` is returned.
    empty_value:
        Result returned if the user submits an empty response while
        :data:`allow_empty` is enabled.
    """

    while True:
        value = input(message).strip()
        if not value:
            if allow_empty:
                return empty_value
            print(error_msg)
            continue
        if validator(value):
            return value
        print(error_msg)
