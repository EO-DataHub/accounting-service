.PHONY: dockerbuild dockerpush dockerrun run test testonce ruff black lint isort pre-commit-check requirements-update requirements setup
VERSION ?= latest
IMAGENAME = accounting-service
DOCKERREPO ?= public.ecr.aws/eodh

dockerbuild:
	DOCKER_BUILDKIT=1 docker build -t ${IMAGENAME}:${VERSION} .

dockerpush: dockerbuild
	docker tag ${IMAGENAME}:${VERSION} ${DOCKERREPO}/${IMAGENAME}:${VERSION}
	docker push ${DOCKERREPO}/${IMAGENAME}:${VERSION}

dockerrun:
	# Once this is working, use 'PYTHONPATH=. python ./tests/send_test_message' to send
	# a test billing event. Use 'PGHOST=localhost PGPORT=5432 psql accounting accountg'
	# with password 'changeme' (or whatever you set in docker-compose.yaml) to view the
	# recorded events.
	docker compose up

run-ingester:
	# You will need to run Pulsar and PostgreSQL before this will work.
	# See tests/send_test_message
	PYTHONPATH=. python -m accounting_service.ingester --pulsar-url pulsar://localhost

test:
	./venv/bin/ptw tests

testonce:
	./venv/bin/pytest

ruff:
	./venv/bin/ruff check .

black:
	./venv/bin/black .

isort:
	./venv/bin/isort . --profile black

validate-pyproject:
	validate-pyproject pyproject.toml

lint: ruff black isort validate-pyproject

requirements.txt: venv pyproject.toml
	./venv/bin/pip-compile

requirements-dev.txt: venv pyproject.toml
	./venv/bin/pip-compile --extra dev -o requirements-dev.txt

requirements: requirements.txt requirements-dev.txt

requirements-update: venv
	./venv/bin/pip-compile -U
	./venv/bin/pip-compile --extra dev -o requirements-dev.txt -U

venv:
	virtualenv -p python3.13 venv
	./venv/bin/python -m ensurepip -U
	./venv/bin/pip3 install pip-tools

.make-venv-installed: venv requirements.txt requirements-dev.txt
	./venv/bin/pip3 install -r requirements.txt -r requirements-dev.txt
	touch .make-venv-installed

.git/hooks/pre-commit:
	./venv/bin/pre-commit install
	curl -o .pre-commit-config.yaml https://raw.githubusercontent.com/EO-DataHub/github-actions/main/.pre-commit-config-python.yaml

setup: venv requirements .make-venv-installed .git/hooks/pre-commit

krestart:
	kubectl rollout restart deployment.apps/accounting-service -n accounting
