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
	uv run python benchmarks/bench.py

##@ Help

.PHONY: help
help: ## Show this help
	@awk 'BEGIN {FS = ":.*##"; printf "\nUsage:\n  make \033[36m<target>\033[0m\n"} /^[a-zA-Z0-9_-]+:.*?##/ { printf "  \033[36m%-12s\033[0m %s\n", $$1, $$2 } /^##@/ { printf "\n\033[1m%s\033[0m\n", substr($$0, 5) }' $(MAKEFILE_LIST)
