# syntax=docker/dockerfile:1@sha256:87999aa3d42bdc6bea60565083ee17e86d1f3339802f543c0d03998580f9cb89
FROM ghcr.io/astral-sh/uv:python3.13-trixie-slim@sha256:f4adb3fe11f03f693466c36afd17b43ffd11eb9df3cd4f7a9337cf2e6ec4c8e8 AS builder

RUN rm -f /etc/apt/apt.conf.d/docker-clean; \
    echo 'Binary::apt::APT::Keep-Downloaded-Packages "true";' > /etc/apt/apt.conf.d/keep-cache

RUN --mount=type=cache,target=/var/cache/apt,sharing=locked \
    --mount=type=cache,target=/var/lib/apt,sharing=locked \
    apt-get update -y \
    && apt-get install --yes --quiet git g++

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


FROM ghcr.io/astral-sh/uv:python3.13-trixie-slim@sha256:f4adb3fe11f03f693466c36afd17b43ffd11eb9df3cd4f7a9337cf2e6ec4c8e8

RUN rm -f /etc/apt/apt.conf.d/docker-clean; \
    echo 'Binary::apt::APT::Keep-Downloaded-Packages "true";' > /etc/apt/apt.conf.d/keep-cache

RUN --mount=type=cache,target=/var/cache/apt,sharing=locked \
    --mount=type=cache,target=/var/lib/apt,sharing=locked \
    apt-get update -y && apt-get upgrade -y \
    && apt-get install --yes --quiet postgresql-client-17

ENV UV_NO_DEV=1

WORKDIR /app

COPY --from=builder /app /app

# Two commands are likely:
#  - python -m accounting_service.ingester      # Runs the ingester
#  - fastapi run accounting_service/app/app.py  # Runs the API server
CMD ["uv", "run", "--no-sync", "fastapi", "run", "accounting_service/app/app.py"]
