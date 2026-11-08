COMPOSE := docker compose
RUN := $(COMPOSE) run --rm app

.PHONY: help up up-all down clean generate run step changes test lint health psql logs

help:            ## list targets
	@grep -E '^[a-z-]+:.*##' $(MAKEFILE_LIST) | awk -F':.*## ' '{printf "  %-10s %s\n", $$1, $$2}'

up:              ## start sources (PostgreSQL, MongoDB, Kafka) and build the pipeline image
	$(COMPOSE) up -d --build postgres mongo kafka
	$(COMPOSE) build app

up-all:          ## start everything, including Airflow, n8n and Superset
	$(COMPOSE) --profile orchestration --profile bi up -d --build

generate:        ## load synthetic data into the three source systems
	$(RUN) python -m c360 generate

run:             ## run the whole pipeline: bronze -> silver -> checks -> gold -> warehouse -> model
	$(RUN) bash scripts/run_pipeline.sh all

step:            ## run one step, e.g. `make step S=silver`
	$(RUN) bash scripts/run_pipeline.sh $(S)

changes:         ## change 2% of customers in core banking, to see SCD Type 2 history on the next run
	$(RUN) python -m c360 change-customers

test:            ## unit and integration tests
	$(RUN) pytest -q

lint:
	$(RUN) ruff check src tests

health:          ## check the source systems are reachable
	$(RUN) bash scripts/healthcheck.sh

psql:            ## open a SQL shell on the warehouse
	$(COMPOSE) exec postgres psql -U c360 -d c360

down:            ## stop containers (data is kept)
	$(COMPOSE) --profile orchestration --profile bi down

clean:           ## stop containers and delete all data
	$(COMPOSE) --profile orchestration --profile bi down -v
