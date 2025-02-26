# syntax=docker/dockerfile:1
FROM python:3.13-slim

RUN rm -f /etc/apt/apt.conf.d/docker-clean; \
    echo 'Binary::apt::APT::Keep-Downloaded-Packages "true";' > /etc/apt/apt.conf.d/keep-cache

RUN --mount=type=cache,target=/var/cache/apt,sharing=locked \
    --mount=type=cache,target=/var/lib/apt,sharing=locked \
    apt-get update -y && apt-get upgrade -y \
    && apt-get install --yes --quiet git postgresql-client-15

WORKDIR /accounting-service
ADD LICENSE requirements.txt ./
ADD accounting_service ./accounting_service/
ADD pyproject.toml ./
RUN --mount=type=cache,target=/root/.cache/pip pip3 install -r requirements.txt .

# Two commands are likely:
#  - python -m accounting_service.ingester      # Runs the ingester
#  - fastapi run accounting_service/app/app.py  # Runs the API server
CMD ["fastapi", "run", "accounting_service/app/app.py"]
