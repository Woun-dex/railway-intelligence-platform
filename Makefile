# =============================================================================
# Railway Intelligence Platform — unified build & run shortcuts
# =============================================================================
# Usage: `make help`
# Note: targets shell out to bash; on Windows run from Git Bash or WSL.

SHELL := /bin/bash
COMPOSE := docker compose
MVN := mvn

.DEFAULT_GOAL := help

## ---- Infrastructure --------------------------------------------------------

.PHONY: up
up: ## Start the backbone (Kafka, Schema Registry, Console) + create topics
	$(COMPOSE) up -d
	@echo "Kafka      -> localhost:9092"
	@echo "Schema Reg -> http://localhost:8081"
	@echo "Console    -> http://localhost:8080"

.PHONY: down
down: ## Stop all infrastructure (keeps volumes)
	$(COMPOSE) down

.PHONY: nuke
nuke: ## Stop infrastructure AND delete Kafka data volume
	$(COMPOSE) down -v

.PHONY: topics
topics: ## List Kafka topics
	docker exec rail-kafka kafka-topics --bootstrap-server localhost:9092 --list

.PHONY: logs
logs: ## Tail infrastructure logs
	$(COMPOSE) logs -f --tail=100

## ---- Build -----------------------------------------------------------------

.PHONY: build
build: ## Compile all modules + generate protobuf sources (skip tests)
	$(MVN) -q clean package -DskipTests

.PHONY: test
test: ## Run unit tests (partitioner + schema-evolution + parser)
	$(MVN) test

.PHONY: schemas
schemas: ## Generate Java from .proto only
	$(MVN) -q -pl shared-schemas clean compile

## ---- Run -------------------------------------------------------------------

.PHONY: run-ingestion
run-ingestion: ## Run the ingestion gateway (expects infra up)
	$(MVN) -q -pl telemetry-ingestion-service spring-boot:run

.PHONY: run-graph
run-graph: ## Run the graph engine (Engine 1: propagation + RAPTOR), :8091
	$(MVN) -q -pl graph-engine-service spring-boot:run -Dspring-boot.run.jvmArguments="-Xmx2g"

.PHONY: fetch-gtfs
fetch-gtfs: ## Download the full real Transilien/IDFM GTFS feed into data/gtfs/
	bash scripts/fetch-gtfs.sh

## ---- Engine 3: predictive STGCN service (Python) ---------------------------

.PHONY: stgcn-protos
stgcn-protos: ## Generate the Python protobuf bindings from shared-schemas
	cd predictive-stgcn-service && bash scripts/gen_protos.sh

.PHONY: stgcn-build
stgcn-build: ## Build the STGCN service image (context = repo root)
	docker compose --profile ml build stgcn-predictor

.PHONY: stgcn-up
stgcn-up: ## Run the STGCN predictor (expects infra up + graph engine on :8091)
	docker compose --profile ml up -d stgcn-predictor
	@echo "STGCN -> http://localhost:8092/health"

.PHONY: stgcn-test
stgcn-test: ## Run the STGCN unit + latency (P99<=35ms) tests
	cd predictive-stgcn-service && pytest -q

.PHONY: stgcn-load
stgcn-load: ## Simulated-load latency probe against the running STGCN service
	cd predictive-stgcn-service && python scripts/loadgen.py --clients 16 --rounds 50 --runs 20

## ---- Validation (DoD) ------------------------------------------------------

.PHONY: smoke
smoke: ## Send one valid + one malformed payload (checks happy path + DLQ)
	@echo "-> valid PositionEvent (expect 202):"
	curl -s -o /dev/null -w "  HTTP %{http_code}\n" -X POST localhost:8090/ingest/position \
	  -H 'Content-Type: application/json' \
	  -d '{"trip_id":"RER-E:T4471","vehicle_id":"V12","lat":48.8443,"lon":2.3743,"delay_seconds":120}'
	@echo "-> malformed JSON (expect 202, lands in rail.raw.dlq):"
	curl -s -o /dev/null -w "  HTTP %{http_code}\n" -X POST localhost:8090/ingest/position \
	  -H 'Content-Type: application/json' \
	  -d '{"trip_id":"BROKEN", this is not json'

.PHONY: load
load: ## Throughput test: bash scripts/loadgen.sh [TOTAL] [BATCH]
	bash scripts/loadgen.sh 20000 2000 8

.PHONY: graph-matrix
graph-matrix: ## Fetch the live network matrix JSON from the graph engine
	curl -s localhost:8091/graph/matrix | head -c 2000; echo

.PHONY: graph-spectral
graph-spectral: ## Fetch the STGCN spectral matrix (Chebyshev-scaled Laplacian)
	curl -s "localhost:8091/graph/spectral?form=scaled" | head -c 2000; echo

.PHONY: cascade-demo
cascade-demo: ## Post a delayed PositionEvent at La Défense to trigger a cascade
	@echo "  → looking up La Defense station index…"
	curl -s -o /dev/null -w "  HTTP %{http_code}\n" -X POST localhost:8090/ingest/position \
	  -H 'Content-Type: application/json' \
	  -d '{"trip_id":"RER-A:demo","lat":48.8918,"lon":2.2386,"delay_seconds":600}'
	@echo "  → watch rail.graph.cascade in Console (http://localhost:8080)"
	@echo "  → find station indices: curl localhost:8091/graph/stations?q=defense"

.PHONY: help
help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | \
	  awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-16s\033[0m %s\n", $$1, $$2}'
