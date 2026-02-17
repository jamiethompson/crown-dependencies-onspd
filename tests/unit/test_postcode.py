from scripts.common.postcode import is_valid_uk_unit_postcode, normalise_postcode


def test_normalise_happy_path():
    assert normalise_postcode("je2 3ab") == "JE2 3AB"


def test_normalise_removes_noise_and_whitespace():
    assert normalise_postcode(" je-2/3ab ") == "JE2 3AB"


def test_normalise_rejects_empty_and_none():
    assert normalise_postcode(None) is None
    assert normalise_postcode("   ") is None


def test_normalise_rejects_too_short_or_long():
    assert normalise_postcode("ABC") is None
    assert normalise_postcode("ABCDEFGHI") is None


def test_normalise_rejects_invalid_pattern():
    assert normalise_postcode("JEA 3AB") is None


def test_normalise_extracts_embedded_postcode_from_address_text():
    raw = "55 - 57 Duke Street Douglas Isle Of Man IM1 2AU"
    assert normalise_postcode(raw) == "IM1 2AU"


def test_validator_accepts_unit_regex():
    assert is_valid_uk_unit_postcode("JE2 3AB")
    assert not is_valid_uk_unit_postcode("JE23AB")
