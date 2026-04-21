"""Nickname validation lives in the Pydantic contracts now."""

import pytest
from pydantic import ValidationError

from contracts import RegisterUserCommand


def test_accepts_valid_nickname() -> None:
    cmd = RegisterUserCommand(
        user_id="11111111-1111-1111-1111-111111111111",  # type: ignore[arg-type]
        nickname="alice_01",
    )
    assert cmd.nickname == "alice_01"


@pytest.mark.parametrize(
    "bad",
    [
        "ab",  # too short
        "A" * 33,  # too long
        "HasUpper",  # uppercase
        "with space",  # space
        "has-dash",  # dash
        "",  # empty
    ],
)
def test_rejects_invalid_nickname(bad: str) -> None:
    with pytest.raises(ValidationError):
        RegisterUserCommand(
            user_id="11111111-1111-1111-1111-111111111111",  # type: ignore[arg-type]
            nickname=bad,
        )
