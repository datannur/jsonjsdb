.PHONY: test lint typecheck check coverage test-cov

test:
	uv run pytest

test-cov:
	uv run pytest --cov=src/jsonjsdb --cov-report=xml --cov-report=html --cov-fail-under=0

coverage:
	uv run pytest --cov=src/jsonjsdb --cov-report=term-missing --cov-report=html

lint:
	uv run ruff check . && uv run ruff format --check .

typecheck:
	uv run pyright src/jsonjsdb tests

check: lint typecheck test-cov
