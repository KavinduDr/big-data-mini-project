FROM python:3.10-slim

WORKDIR /app

# build-essential/libpq-dev are needed to build wheels that have no manylinux
# build for this platform; curl is only used by the container healthcheck.
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    libpq-dev \
    curl \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

# Unbuffered stdout/stderr so structured logs reach `docker compose logs`
# immediately (observability requirement).
ENV PYTHONPATH=/app \
    PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1

# Run as an unprivileged user. The directories that receive mounted volumes are
# pre-created and owned by that user so Docker initialises the named volumes
# with the right ownership.
RUN useradd --create-home --shell /bin/bash fleet \
    && mkdir -p /app/data_lake /app/storage \
    && chown -R fleet:fleet /app
USER fleet

EXPOSE 8000
EXPOSE 8501

# NOTE: no HEALTHCHECK here on purpose - this image is shared by the producer,
# speed layer, batch layer and API services, so a container level HTTP probe
# would mark the non-API services unhealthy. The API healthcheck is declared on
# the `api` service in docker-compose.yml instead.

# Default: serve the API. Compose overrides this per service.
CMD ["uvicorn", "serving.api:app", "--host", "0.0.0.0", "--port", "8000"]
