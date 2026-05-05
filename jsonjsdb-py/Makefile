.PHONY: test lint typecheck check coverage test-cov diff-cov

test:
	uv run pytest

test-cov:
	uv run pytest --cov=src/jsonjsdb --cov-report=xml --cov-report=html --cov-fail-under=0

diff-cov:
	@git rev-parse --verify --quiet $${DIFF_COVER_COMPARE_BRANCH:-origin/main} >/dev/null || git fetch origin main:refs/remotes/origin/main
	uv run diff-cover coverage.xml --compare-branch=$${DIFF_COVER_COMPARE_BRANCH:-origin/main} --fail-under=100

coverage:
	uv run pytest --cov=src/jsonjsdb --cov-report=term-missing --cov-report=html

lint:
	uv run ruff check . && uv run ruff format --check .

typecheck:
	uv run pyright src/jsonjsdb tests

check: lint typecheck test-cov diff-cov
