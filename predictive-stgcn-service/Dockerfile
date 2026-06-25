# Predictive STGCN service (Engine 3).
# Build context is the REPOSITORY ROOT so the shared protobuf schemas can be
# copied in and compiled here:
#     docker build -f predictive-stgcn-service/Dockerfile -t rail-stgcn .
# (docker-compose sets `context: .` / `dockerfile: predictive-stgcn-service/Dockerfile`.)
FROM python:3.11-slim AS base

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
        build-essential curl && rm -rf /var/lib/apt/lists/*

# Install Python deps first (layer cache). CPU-only torch keeps the image lean.
COPY predictive-stgcn-service/requirements.txt .
RUN pip install --extra-index-url https://download.pytorch.org/whl/cpu -r requirements.txt

# App source + shared protobuf contracts.
COPY predictive-stgcn-service/ /app/
COPY shared-schemas/src/main/proto /protos

# Generate the Python protobuf bindings from the single source of truth.
RUN python -m grpc_tools.protoc -I /protos --python_out=/app/src/proto /protos/*.proto \
    && touch /app/src/proto/__init__.py

EXPOSE 8092
HEALTHCHECK --interval=15s --timeout=5s --start-period=40s --retries=5 \
    CMD curl -fsS http://localhost:8092/health | grep -q '"status": *"ready"' || exit 1

CMD ["python", "-m", "uvicorn", "src.main:app", "--host", "0.0.0.0", "--port", "8092"]
