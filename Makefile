VERSION ?= latest
IMAGENAME = accounting-service
DOCKERREPO ?= public.ecr.aws/eodh
uv-run ?= uv run --no-sync

.PHONY: dockerbuild
dockerbuild:
	DOCKER_BUILDKIT=1 docker build -t ${IMAGENAME}:${VERSION} .

.PHONY: dockerpush
dockerpush: dockerbuild
	docker tag ${IMAGENAME}:${VERSION} ${DOCKERREPO}/${IMAGENAME}:${VERSION}
	docker push ${DOCKERREPO}/${IMAGENAME}:${VERSION}

.PHONY: dockerrun
dockerrun:
	docker compose up

.PHONY: run-ingester
run-ingester:
	${uv-run} python -m accounting_service.ingester --pulsar-url pulsar://localhost

.PHONY: test
test:
	${uv-run} ptw .

.PHONY: testonce
testonce:
	${uv-run} pytest

# The tests that need no database. Under two seconds, no container, so this is the one
# to run on every change. tests/integration holds the rest.
.PHONY: test-unit
test-unit:
	${uv-run} pytest tests --ignore=tests/integration

.PHONY: test-integration
test-integration:
	${uv-run} pytest tests/integration

.git/hooks/pre-commit:
	${uv-run} pre-commit install
	curl -o .pre-commit-config.yaml https://raw.githubusercontent.com/EO-DataHub/github-actions/main/.pre-commit-config-python.yaml

.PHONY: setup
setup: update .git/hooks/pre-commit

.PHONY: pre-commit
pre-commit:
	${uv-run} pre-commit

.PHONY: pre-commit-all
pre-commit-all:
	${uv-run} pre-commit run --all-files

# Needs Docker. Starts a throwaway PostgreSQL, migrates it from empty, and runs `alembic
# check` against it - both as subprocesses, which is what makes an empty target_metadata
# visible. See the docstring in dev/check_migrations.py.
.PHONY: check-migrations
check-migrations:
	${uv-run} python dev/check_migrations.py

.PHONY: check
check:
	${uv-run} ruff check
	${uv-run} ruff format --check --diff
	${uv-run} pyright
	${uv-run} validate-pyproject pyproject.toml
	$(MAKE) check-migrations

.PHONY: format
format:
	${uv-run} ruff check --fix
	${uv-run} ruff format

.PHONY: install
install:
	uv sync --frozen

.PHONY: update
update:
	uv sync

.PHONY: krestart
krestart:
	kubectl rollout restart deployment.apps/accounting-api -n accounting
	kubectl rollout restart deployment.apps/accounting-ingester -n accounting
