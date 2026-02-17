# crown-dependencies-onspd

Deterministic pipeline to produce unit-level postcode CSV outputs for Jersey, Guernsey, and the Isle of Man, including strict ONSPD-compatible exports.

## Runtime
- Python 3.12+
- Linux/macOS (CI runs on Ubuntu)

## Quick Start
```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
make all
```

## CLI
```bash
python -m scripts.cli <command> [options]
```

Commands:
- `discover`
- `harvest`
- `merge`
- `map-onspd`
- `validate`
- `all`

Common options:
- `--territory JE|GY|IM|all`
- `--run-date YYYY-MM-DD`
- `--run-id STRING`
- `--config-dir PATH`
- `--data-dir PATH`
- `--strict`

## Make Targets
- `make discover`
- `make harvest`
- `make merge`
- `make map-onspd`
- `make validate`
- `make all`
- `make test`

## Current Status
Stages 1-3 are in place: scaffold, config loading, CLI orchestration, source ingestion modules, deterministic merge, coordinate resolution, scoring, temporal tracking, canonical export, and strict ONSPD mapping.

## License Notes
See `LICENSE_NOTES.md` for source attribution and usage constraints.
