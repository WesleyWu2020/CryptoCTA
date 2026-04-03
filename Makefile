.PHONY: setup test lint check docs-check docs-drift-check

setup:
	python -m pip install -U pip
	python -m pip install -e ".[dev]"

test:
	PYTHONPATH=src pytest -q

lint:
	ruff check src tests scripts

docs-check:
	@test -f AGENTS.md
	@test -f PROGRESS.md
	@test -f DECISIONS.md
	@test -f src/cta_core/strategy_runtime/ARCHITECTURE.md
	@test -f src/cta_core/data/CONSTRAINTS.md
	@test -f src/cta_core/execution/CONSTRAINTS.md
	@test -f src/cta_core/risk/CONSTRAINTS.md
	@echo "docs-check passed"

docs-drift-check:
	./scripts/check_module_docs.sh "$(BASE_SHA)" "$(HEAD_SHA)"

check: docs-check lint test
