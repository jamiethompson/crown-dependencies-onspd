"""CLI entrypoint for crown dependencies postcode pipeline."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

from scripts.common.config_loader import load_all_configs, resolve_territories
from scripts.common.constants import EXIT_HARD_FAIL, EXIT_PARTIAL, EXIT_SUCCESS, STAGES
from scripts.common.errors import PipelineError
from scripts.common.ids import generate_run_id
from scripts.common.logging import build_logger, log_event
from scripts.common.time_utils import parse_run_date
from scripts.discovery.arcgis_discover import run_discovery
from scripts.harvest.runner import run_harvest_for_territory
from scripts.pipeline.export import write_canonical_csv
from scripts.pipeline.map_to_onspd import run_map_onspd
from scripts.pipeline.normalise_merge import run_normalise_merge
from scripts.pipeline.reports import write_run_summary
from scripts.pipeline.temporal import apply_temporal_tracking
from scripts.pipeline.validate import run_validate


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=[*STAGES, "all"])
    parser.add_argument("--territory", default="all", choices=["JE", "GY", "IM", "all"])
    parser.add_argument("--run-date", default=None)
    parser.add_argument("--run-id", default=None)
    parser.add_argument("--config-dir", default="./config")
    parser.add_argument("--overlay-config-dir", default=None)
    parser.add_argument("--data-dir", default="./data")
    parser.add_argument("--log-level", default="INFO", choices=["DEBUG", "INFO", "WARN", "ERROR"])
    parser.add_argument("--strict", action="store_true")
    return parser.parse_args(argv)


def execute_stage(stage: str, territory_code: str, cfg: dict, bundle, data_dir: Path, run_id: str, run_date: str):
    if stage == "discover":
        run_discovery(territory_code, cfg, data_dir, run_id)
    elif stage == "harvest":
        run_harvest_for_territory(territory_code, cfg, data_dir, run_id, run_date)
    elif stage == "merge":
        merged = run_normalise_merge(territory_code, cfg, bundle.scoring_rules, data_dir, run_id)
        canonical_path = data_dir / "out" / cfg["output"]["canonical_filename"]
        state_path = data_dir / "state" / "first_last_seen" / f"{territory_code.lower()}.json"
        rows_with_temporal, _stats = apply_temporal_tracking(
            merged["rows"],
            territory_code=territory_code,
            canonical_output_path=canonical_path,
            state_path=state_path,
            run_date=run_date,
        )
        write_canonical_csv(cfg, data_dir, rows_with_temporal)
    elif stage == "map-onspd":
        run_map_onspd(territory_code, cfg, bundle.onspd_columns, data_dir)
    elif stage == "validate":
        run_validate(territory_code, cfg, bundle.onspd_columns, data_dir, run_id, run_date)
    else:
        raise ValueError(f"Unknown stage: {stage}")


def run_command(args: argparse.Namespace) -> int:
    run_id = args.run_id or generate_run_id()
    run_date = parse_run_date(args.run_date)
    config_dir = Path(args.config_dir)
    overlay_config_dir = Path(args.overlay_config_dir) if args.overlay_config_dir else None
    data_dir = Path(args.data_dir)

    logger = build_logger(run_id, data_dir=data_dir, level=args.log_level)
    bundle = load_all_configs(config_dir, overlay_config_dir=overlay_config_dir)
    territories = resolve_territories(args.territory)
    stages = STAGES if args.command == "all" else (args.command,)

    had_partial_failure = False

    for stage in stages:
        log_event(logger, "stage start", run_id=run_id, stage=stage, event="STAGE_START", status="ok")
        for territory_code in territories:
            cfg = bundle.territories[territory_code]
            try:
                execute_stage(stage, territory_code, cfg, bundle, data_dir, run_id, run_date)
            except PipelineError as exc:
                had_partial_failure = True
                log_event(
                    logger,
                    f"stage failed for territory {territory_code}",
                    run_id=run_id,
                    stage=stage,
                    territory=territory_code,
                    event="STAGE_FAIL",
                    status="error",
                    error_code=exc.error_code,
                )
                if exc.error_code == "CONTRACT_ERROR":
                    return EXIT_HARD_FAIL
                if args.strict:
                    return EXIT_HARD_FAIL
            except Exception:
                had_partial_failure = True
                log_event(
                    logger,
                    f"unexpected failure for territory {territory_code}",
                    run_id=run_id,
                    stage=stage,
                    territory=territory_code,
                    event="STAGE_FAIL",
                    status="error",
                    error_code="UNEXPECTED_ERROR",
                )
                if args.strict:
                    return EXIT_HARD_FAIL
        log_event(logger, "stage end", run_id=run_id, stage=stage, event="STAGE_END", status="ok")

    write_run_summary(data_dir, run_id=run_id, run_date=run_date, territories=territories)
    if had_partial_failure:
        return EXIT_PARTIAL
    return EXIT_SUCCESS


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv or sys.argv[1:])
    try:
        return run_command(args)
    except PipelineError:
        return EXIT_HARD_FAIL
    except Exception:
        return EXIT_HARD_FAIL


if __name__ == "__main__":
    raise SystemExit(main())
