from scripts.common.scoring import apply_scoring_profile


def test_apply_scoring_profile_applies_expected_rules():
    profile = {
        "rules": [
            {"id": "authoritative_presence", "when": "has_source(authoritative)", "add": 50},
            {"id": "osm_presence", "when": "has_source(osm)", "add": 10},
            {"id": "authoritative_coords", "when": "coord_source(authoritative)", "add": 15},
        ],
        "clamp": {"min": 0, "max": 100},
    }

    score, explanation = apply_scoring_profile(
        profile,
        source_classes={"authoritative", "osm"},
        coordinate_source="authoritative",
    )

    assert score == 75
    assert explanation["applied_rules"] == [
        "authoritative_presence",
        "osm_presence",
        "authoritative_coords",
    ]


def test_apply_scoring_profile_clamps():
    profile = {
        "rules": [{"id": "huge", "when": "has_source(osm)", "add": 500}],
        "clamp": {"min": 0, "max": 100},
    }
    score, _ = apply_scoring_profile(profile, source_classes={"osm"}, coordinate_source="osm")
    assert score == 100
