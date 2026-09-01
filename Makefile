PY ?= .venv/bin/python

.DEFAULT_GOAL := help

##@ Development

.PHONY: build
build: ## Build the whole workspace
	cargo build --workspace

.PHONY: test
test: ## Run the Rust and Python test suites
	cargo test --workspace
	uv run pytest

.PHONY: fmt
fmt: ## Format all crates
	cargo fmt --all

.PHONY: lint
lint: ## Clippy over all targets with warnings denied
	cargo clippy --workspace --all-targets -- -D warnings

##@ Python

.PHONY: wheel
wheel: ## Build the release wheel with maturin
	uv run maturin build --release

.PHONY: develop
develop: ## Install the Python package into the active venv
	uv run maturin develop

.PHONY: bench
bench: ## Run the benchmark suite against aiofiles
	$(PY) benchmarks/bench.py $(if $(FS),--dir $(FS))

##@ Performance

.PHONY: ladder
ladder: ## Latency ladder: attribute per-op cost to a layer (FS=dir to pick a filesystem)
	$(PY) perf/ladder.py --batch 2000 --reps 20 $(if $(FS),--dir $(FS))

.PHONY: counters
counters: ## Hardware counters per ladder rung (needs CAP_PERFMON on perf)
	perf/counters.sh $(if $(FS),--dir $(FS)) 3

.PHONY: flame
flame: ## Sampled profile of one rung: make flame RUNG=read
	perf/flame.sh $(if $(FS),--dir $(FS)) $(or $(RUNG),read) 5

.PHONY: uring
uring: ## io_uring tracepoint counts for a running pid: make uring PID=1234
	bpftrace perf/uring.bt $(PID) 5

.PHONY: wake
wake: ## Ground-truth cross-thread wake cost on this machine
	$(PY) perf/wake_probe.py

##@ Help

.PHONY: help
help: ## Show this help
	@awk 'BEGIN {FS = ":.*##"; printf "\nUsage:\n  make \033[36m<target>\033[0m\n"} /^[a-zA-Z0-9_-]+:.*?##/ { printf "  \033[36m%-12s\033[0m %s\n", $$1, $$2 } /^##@/ { printf "\n\033[1m%s\033[0m\n", substr($$0, 5) }' $(MAKEFILE_LIST)
