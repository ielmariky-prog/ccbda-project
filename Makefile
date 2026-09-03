# proj1102 - common developer commands, one target per workflow.

SHELL    := /bin/bash
PROFILE  ?= ralph
REGION   ?= eu-west-1
VENV_PY  := sentiment/.venv/bin/python

.DEFAULT_GOAL := help

.PHONY: help check check-python check-bash check-asl test \
        deploy-hourly deploy-daily deploy-quicksight deploy-stepfunctions \
        health backtest \
        docker-build docker-run \
        clean

help:
	@echo "proj1102 - common operations"
	@echo ""
	@echo "  make check               - run all static checks (Python + Bash + ASL JSON)"
	@echo "  make test                - run the pytest unit-test suite"
	@echo "  make health              - query AWS for live pipeline health (alarms, schedules, ages)"
	@echo "  make backtest            - run the daily-model walk-forward backtest"
	@echo ""
	@echo "  make docker-build        - build the housekeeping admin container"
	@echo "  make docker-run          - run the health probe inside Docker (mounts ~/.aws ro)"
	@echo ""
	@echo "  make deploy-hourly       - hourly forecast + sentiment + retrain + freshness"
	@echo "  make deploy-daily        - daily ingest + forecast + retrain"
	@echo "  make deploy-quicksight   - refresh Lambda + Athena tables"
	@echo "  make deploy-stepfunctions - hourly + daily state machines"
	@echo ""
	@echo "  PROFILE=$(PROFILE)  REGION=$(REGION)"

# Static checks (mirrors the CI workflow locally)

check: check-python check-bash check-asl

check-python:
	@echo "-> py_compile on all Python sources"
	@find . -path './.git' -prune -o -path '*/.venv' -prune -o -name '*.py' -print \
	  | xargs -r python3 -m py_compile
	@echo "  OK"

check-bash:
	@echo "-> bash -n on all deploy scripts"
	@for f in $$(find . -name 'deploy*.sh' -not -path '*/.venv/*'); do \
	    echo "  $$f"; bash -n "$$f"; \
	  done
	@echo "  OK"

check-asl:
	@echo "-> JSON validation on Step Functions ASL"
	@for f in stepfunctions/*.asl.json; do \
	    python3 -c "import json,sys; json.load(open('$$f'))" \
	      && echo "  OK $$f" || (echo "  FAIL $$f"; exit 1); \
	  done

test:
	@$(VENV_PY) -m pytest -ra tests/

# Health probe - calls the housekeeping admin script

health:
	@$(VENV_PY) housekeeping/check_pipeline.py

backtest:
	@$(VENV_PY) scripts/backtest_daily.py

# Docker - housekeeping admin probe in a portable container

docker-build:
	docker build -t proj1102-housekeeping -f housekeeping/Dockerfile .

docker-run:
	docker run --rm \
	  -v $$HOME/.aws:/home/probe/.aws:ro \
	  -e PROFILE=$(PROFILE) -e REGION=$(REGION) \
	  proj1102-housekeeping

# Deploys - each one calls the component's own idempotent deploy.sh

deploy-hourly:
	cd lambda/sentiment   && bash deploy.sh
	cd lambda/forecast    && python3 create_table.py || true
	cd lambda/forecast    && bash deploy.sh
	cd lambda/retrain     && bash deploy.sh
	cd lambda/freshness   && bash deploy.sh

deploy-daily:
	cd lambda/daily_ingest    && bash deploy.sh
	cd lambda/forecast_daily  && bash deploy.sh
	cd lambda/daily_retrain   && bash deploy.sh

deploy-quicksight:
	cd quicksight && bash deploy.sh

deploy-stepfunctions:
	cd stepfunctions && bash deploy.sh --with-cutover
	cd stepfunctions && bash deploy_daily.sh --with-cutover


clean:
	@find . -type d -name '__pycache__' -not -path '*/.venv/*' -exec rm -rf {} + 2>/dev/null || true
	@echo "Removed __pycache__ directories."
