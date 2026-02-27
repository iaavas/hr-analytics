FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /app

RUN apt-get update && \
    apt-get install -y --no-install-recommends build-essential libpq-dev curl && \
    rm -rf /var/lib/apt/lists/*

COPY pyproject.toml README.md alembic.ini ./
COPY src ./src
COPY alembic ./alembic
COPY dashboard ./dashboard
COPY data ./data

RUN pip install --upgrade pip && \
    pip install .

RUN mkdir -p /app/logs/manifests /app/logs/markers /app/dashboard/output

EXPOSE 5173

CMD ["uvicorn", "src.app.main:app", "--host", "0.0.0.0", "--port", "5173"]
