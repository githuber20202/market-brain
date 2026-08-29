# syntax=docker/dockerfile:1
FROM python:3.12-slim
WORKDIR /app
COPY pyproject.toml README.md /app/
COPY src /app/src
RUN pip install --no-cache-dir .
COPY config /app/config
COPY data /app/data
COPY scripts /app/scripts
COPY web /app/web
ENV PYTHONPATH=/app/src
CMD ["uvicorn", "market_brain.api.main:app", "--host", "0.0.0.0", "--port", "8080"]
