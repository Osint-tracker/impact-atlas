"""Authorization tests without production data or real credentials."""

import pytest

from impact_atlas.admin_security import AdminAccess


def test_tokens_are_hidden_and_roles_are_distinct():
    access = AdminAccess(writer_token="writer-" + "x" * 32, reader_token="reader-" + "y" * 32)
    assert "xxxx" not in repr(access)
    assert access.role_for("Bearer " + access.writer_token) == "writer"
    assert access.role_for("bearer " + access.reader_token) == "reader"
    for header in (None, "", "Basic ignored", "Bearer " + "z" * 40, "Bearer ü" * 40):
        assert access.role_for(header) is None


@pytest.mark.parametrize("token", ["", "short", "x" * 257, "x" * 40 + "\n"])
def test_invalid_token_configuration_fails_closed(token):
    with pytest.raises(ValueError):
        AdminAccess(writer_token=token)


def test_identical_role_tokens_are_rejected():
    with pytest.raises(ValueError):
        AdminAccess(writer_token="x" * 40, reader_token="x" * 40)
