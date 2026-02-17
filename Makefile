PYTHON ?= python3
CLI := $(PYTHON) -m scripts.cli

discover:
	$(CLI) discover --territory all

harvest:
	$(CLI) harvest --territory all

merge:
	$(CLI) merge --territory all

map-onspd:
	$(CLI) map-onspd --territory all

validate:
	$(CLI) validate --territory all

all:
	$(CLI) all --territory all

test:
	pytest -q
