from pyproj import Transformer

from scripts.pipeline.coordinates import resolve_best_coordinate


def _territory_config():
    return {
        "validation": {
            "bbox_wgs84": {
                "min_lat": 49.0,
                "max_lat": 50.0,
                "min_lon": -3.0,
                "max_lon": -1.0,
            }
        },
        "crs": {"default_epsg": 4326, "authoritative_epsg_hint_by_source": {}},
        "source_priority": ["auth", "osm"],
    }


def test_coordinate_precedence_prefers_authoritative():
    records = [
        {"raw_lat": 49.21, "raw_lon": -2.11, "source_class": "osm", "source_name": "osm", "source_record_id": "2", "source_wkid": 4326},
        {"raw_lat": 49.2, "raw_lon": -2.1, "source_class": "authoritative", "source_name": "auth", "source_record_id": "1", "source_wkid": 4326},
    ]

    result = resolve_best_coordinate(records, _territory_config())

    assert result["has_coordinates"] is True
    assert result["coordinate_source"] == "authoritative"
    assert result["lat"] == 49.2


def test_coordinate_transform_from_non_wgs84():
    transformer = Transformer.from_crs("EPSG:4326", "EPSG:3857", always_xy=True)
    x, y = transformer.transform(-2.1, 49.2)

    records = [
        {"raw_lat": y, "raw_lon": x, "source_class": "authoritative", "source_name": "auth", "source_record_id": "1", "source_wkid": 3857}
    ]
    result = resolve_best_coordinate(records, _territory_config())

    assert result["has_coordinates"] is True
    assert abs(result["lat"] - 49.2) < 1e-5
    assert abs(result["lon"] - (-2.1)) < 1e-5


def test_coordinate_outlier_sets_note_and_nulls():
    records = [
        {"raw_lat": 10.0, "raw_lon": 10.0, "source_class": "authoritative", "source_name": "auth", "source_record_id": "1", "source_wkid": 4326}
    ]

    result = resolve_best_coordinate(records, _territory_config())
    assert result["has_coordinates"] is False
    assert "COORDINATE_OUTLIER" in result["notes"]
