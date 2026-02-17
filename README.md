# crown-dependencies-onspd

Deterministic pipeline to produce unit-level postcode CSV outputs for Jersey, Guernsey, and the Isle of Man, including strict ONSPD-compatible exports.

## Runtime
- Python 3.12+
- Linux/macOS (CI runs on Ubuntu)

## Install
```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

## Stage Commands
```bash
python -m scripts.cli discover --territory all
python -m scripts.cli harvest --territory all
python -m scripts.cli merge --territory all
python -m scripts.cli map-onspd --territory all
python -m scripts.cli validate --territory all
python -m scripts.cli all --territory all
```

Equivalent Make targets:
- `make discover`
- `make harvest`
- `make merge`
- `make map-onspd`
- `make validate`
- `make all`
- `make test`

## Output Paths
- Canonical CSVs: `data/out/*.csv`
- ONSPD CSVs: `data/out/*_onspd.csv`
- Territory reports: `data/out/reports/*_report.json`
- Run summary: `data/out/reports/run_summary.json`
- Temporal state: `data/state/first_last_seen/*.json`
- Run logs: `data/run_meta/<run_id>.log.jsonl`

## Determinism
Given identical config, identical raw inputs, and identical run-date, canonical and ONSPD outputs are byte-stable.

## Fail-Soft Behaviour
- Source-level failures are tolerated during harvest when other enabled sources succeed.
- If all enabled sources fail for a territory, that territory stage fails.
- ONSPD contract mismatches hard-fail the run with exit code 20.

## Troubleshooting
- Empty canonical output: inspect `data/raw/*` and `data/intermediate/*_canonical.json`.
- ONSPD contract failure: compare `config/onspd_columns.yml` with output headers.
- Missing coordinates: check source WKID values and CRS hints in territory config.
- High invalid counts: inspect `invalid_samples` in intermediate and territory reports.

## Status
Stages 1-4 are implemented: scaffold, config loading, source ingestion, deterministic merge/scoring/temporal logic, strict exports, validation reporting, and CI.

## License Notes
See `LICENSE_NOTES.md` for source attribution and usage constraints.
