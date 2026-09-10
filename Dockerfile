# syntax=docker/dockerfile:1.7
# Multi-stage build: hash-pinned dependency install, non-root runtime.
# Same image serves the HTTP tier and the consumer loop (see CMD / docs).

FROM python:3.12-slim AS builder
WORKDIR /build
COPY requirements.lock ./
RUN python -m venv /opt/venv \
    && /opt/venv/bin/pip install --no-cache-dir --require-hashes -r requirements.lock

FROM python:3.12-slim AS runtime
ENV PATH="/opt/venv/bin:$PATH" \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1
RUN useradd --system --uid 10001 --create-home cloudscale
WORKDIR /app
COPY --from=builder /opt/venv /opt/venv
COPY cloudscale ./cloudscale
COPY cqrs ./cqrs
COPY pyproject.toml README.md ./
USER cloudscale
EXPOSE 8000

# Required at runtime: CLOUDSCALE_JWT_SECRET (>= 32 bytes) and either
#   CLOUDSCALE_STORAGE=sqlite   + CLOUDSCALE_LOG_DB + CLOUDSCALE_PROJECTION_DB
#   CLOUDSCALE_STORAGE=postgres + CLOUDSCALE_PG_DSN
# Consumer process: override CMD with
#   python -m cloudscale.entrypoints.consumer_loop
HEALTHCHECK --interval=15s --timeout=3s --retries=3 \
    CMD python -c "import urllib.request,sys; sys.exit(0 if urllib.request.urlopen('http://127.0.0.1:8000/v1/health', timeout=2).status==200 else 1)"
CMD ["uvicorn", "--factory", "cloudscale.entrypoints.http.main:build_app", \
     "--host", "0.0.0.0", "--port", "8000", "--log-level", "warning"]
