# syntax=docker/dockerfile:1
FROM ghcr.io/astral-sh/uv:python3.13-trixie-slim

RUN rm -f /etc/apt/apt.conf.d/docker-clean; \
    echo 'Binary::apt::APT::Keep-Downloaded-Packages "true";' > /etc/apt/apt.conf.d/keep-cache

RUN --mount=type=cache,target=/var/cache/apt,sharing=locked \
    --mount=type=cache,target=/var/lib/apt,sharing=locked \
    apt-get update -y && apt-get upgrade -y \
    && apt-get install --yes --quiet git postgresql-client-17

ENV UV_NO_DEV=1

WORKDIR /app

# Install dependencies
RUN --mount=type=cache,target=/root/.cache/uv \
    --mount=type=bind,source=uv.lock,target=uv.lock \
    --mount=type=bind,source=pyproject.toml,target=pyproject.toml \
    uv sync --frozen --no-install-project

# Copy project files
COPY . /app

# Sync the project
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --frozen

# Two commands are likely:
#  - python -m accounting_service.ingester      # Runs the ingester
#  - fastapi run accounting_service/app/app.py  # Runs the API server
CMD ["uv", "run", "--no-sync", "fastapi", "run", "accounting_service/app/app.py"]
