"""Normalise postcode values and merge deterministic canonical rows."""

from __future__ import annotations

from collections import defaultdict
from pathlib import Path

from scripts.common.fs import read_json, write_json
from scripts.common.postcode import normalise_postcode
from scripts.common.scoring import apply_scoring_profile
from scripts.pipeline.coordinates import resolve_best_coordinate

RAW_SOURCES = {
    "arcgis": "raw/arcgis/{territory}_arcgis.json",
    "overpass": "raw/osm/overpass/{territory}_overpass.json",
    "geofabrik": "raw/osm/geofabrik/{territory}_geofabrik.json",
}


def _load_source_rows(data_dir: Path, territory_code: str) -> list[dict]:
    rows: list[dict] = []
    territory = territory_code.lower()
    for template in RAW_SOURCES.values():
        path = data_dir / template.format(territory=territory)
        if not path.exists():
            continue
        payload = read_json(path)
        rows.extend(payload.get("rows", []))
    return rows


def _priority_lookup(source_priority: list[str]) -> dict[str, int]:
    return {name: idx for idx, name in enumerate(source_priority)}


def run_normalise_merge(
    territory_code: str,
    territory_config: dict,
    scoring_rules: dict,
    data_dir: Path,
    run_id: str,
) -> dict:
    raw_rows = _load_source_rows(data_dir, territory_code)
    source_priority = _priority_lookup(territory_config["source_priority"])

    invalid_count_by_source: dict[str, int] = defaultdict(int)
    invalid_samples: list[dict] = []
    grouped: dict[str, list[dict]] = defaultdict(list)

    for raw in raw_rows:
        normalised = normalise_postcode(raw.get("raw_postcode"))
        if normalised is None:
            source_name = raw.get("source_name", "unknown")
            invalid_count_by_source[source_name] += 1
            if len(invalid_samples) < 50:
                invalid_samples.append(
                    {
                        "source_name": source_name,
                        "raw_postcode": raw.get("raw_postcode"),
                    }
                )
            continue

        enriched = dict(raw)
        enriched["normalised_postcode"] = normalised
        grouped[normalised].append(enriched)

    sorted_keys = sorted(grouped)
    profile_name = territory_config.get("scoring_profile", "default")
    profile = scoring_rules["profiles"][profile_name]

    canonical_rows: list[dict] = []
    score_explanations: dict[str, dict] = {}

    for key in sorted_keys:
        records = sorted(
            grouped[key],
            key=lambda row: (
                row.get("territory", territory_code),
                key,
                row.get("source_name", ""),
                row.get("source_record_id") or "",
            ),
        )

        ranked_records = sorted(
            records,
            key=lambda row: (
                source_priority.get(row.get("source_name", ""), 9999),
                row.get("raw_postcode") or "",
            ),
        )
        representative = ranked_records[0]

        unique_sources = sorted(
            {row.get("source_name", "") for row in records},
            key=lambda src: (source_priority.get(src, 9999), src),
        )
        source_classes = {row.get("source_class", "other") for row in records}

        coordinate = resolve_best_coordinate(records, territory_config)
        notes = list(coordinate.get("notes", []))

        if "authoritative" not in source_classes:
            notes.append("OSM_BASELINE_ONLY")
        if not coordinate["has_coordinates"]:
            notes.append("COORDINATES_MISSING")
        if territory_code == "IM" and territory_config.get("validation", {}).get("permission_needed_possible_for_iom"):
            notes.append("PERMISSION_NEEDED_POSSIBLE")

        confidence_score, explanation = apply_scoring_profile(
            profile,
            source_classes=source_classes,
            coordinate_source=coordinate.get("coordinate_source"),
        )
        score_explanations[key] = explanation

        canonical_rows.append(
            {
                "territory": territory_code,
                "postcode": representative.get("raw_postcode") or key,
                "normalised_postcode": key,
                "source_list": ";".join(unique_sources),
                "source_count": len(unique_sources),
                "has_coordinates": bool(coordinate["has_coordinates"]),
                "lat": coordinate.get("lat"),
                "lon": coordinate.get("lon"),
                "coordinate_source": coordinate.get("coordinate_source"),
                "confidence_score": confidence_score,
                "first_seen": "",
                "last_seen": "",
                "notes": ";".join(sorted(set(notes))) if notes else None,
            }
        )

    payload = {
        "territory": territory_code,
        "run_id": run_id,
        "raw_row_count": len(raw_rows),
        "valid_postcodes": sum(len(v) for v in grouped.values()),
        "unique_postcodes": len(canonical_rows),
        "invalid_postcodes": dict(sorted(invalid_count_by_source.items())),
        "invalid_samples": invalid_samples,
        "score_explanations": score_explanations,
        "rows": canonical_rows,
    }

    out_path = data_dir / "intermediate" / f"{territory_code.lower()}_canonical.json"
    write_json(out_path, payload)
    return payload
